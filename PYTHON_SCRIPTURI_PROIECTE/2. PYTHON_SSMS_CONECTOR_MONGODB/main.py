from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import pyodbc
from datetime import datetime
MONGO_URI="mongodb://localhost:27017"
SQL_SERVER="localhost"
SQL_DATABASE="DWH_Nr1_Comun"
DB_NORMALIZAT="nr1_clienti"
DB_DENORMALIZAT="nr1_denormalizat"
def conectare_mongodb():
    try:
        client=MongoClient(MONGO_URI,serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print("Conectare MongoDB reusita")
        return client
    except ConnectionFailure:
        print("Conectare MongoDB esuata")
        exit()
def conectare_sql():
    return pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};"+f"SERVER={SQL_SERVER};"+f"DATABASE={SQL_DATABASE};"+"Trusted_Connection=yes;")
mongo=conectare_mongodb()
db_norm=mongo[DB_NORMALIZAT]
db_denorm=mongo[DB_DENORMALIZAT]
conn=conectare_sql()
cursor=conn.cursor()
def val(doc,*chei):
    for c in chei:
        if c in doc and doc[c] is not None:
            return str(doc[c])
    return None
def data_sql(x):
    if not x:
        return None
    if isinstance(x,datetime):
        return x.date()
    try:
        return datetime.fromisoformat(str(x)[:10]).date()
    except:
        return None
def exista(sql,*params):
    cursor.execute(sql,params)
    return cursor.fetchone()[0]>0
def executa(sql):
    cursor.execute(sql)
    conn.commit()
def verifica_tabele():
    tabele=[("stg","Clienti_Normalizat"),("stg","Clienti_Denormalizat"),("stg","Produse_Normalizat"),("stg","Produse_Denormalizat"),("stg","Comenzi_Normalizat"),("stg","Comenzi_Denormalizat"),("stg","Companii_Normalizat"),("stg","Companii_Denormalizat"),("dwh","DimClient"),("dwh","DimCompanie"),("dwh","DimProdus"),("dwh","DimTimp"),("dwh","FactVanzariComune"),("kpi","KPI_Clienti_B2C"),("kpi","KPI_Companii_B2B"),("kpi","KPI_Integrat"),("kpi","KPI_Produse_Promotii")]
    lipsa=[]
    for schema,tabel in tabele:
        if not exista("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",schema,tabel):
            lipsa.append(schema+"."+tabel)
    if lipsa:
        print("Lipsesc tabele:")
        for t in lipsa:
            print(t)
        inchide()
        exit()
def incarca_clienti_normalizat():
    n=0
    for c in db_norm["clienti"].find():
        id_client=val(c,"id_client","cod_client","_id")
        if not id_client or exista("SELECT COUNT(*) FROM stg.Clienti_Normalizat WHERE id_client=?",id_client):
            continue
        cursor.execute("INSERT INTO stg.Clienti_Normalizat(id_client,nume,prenume,email,telefon,magazin_preferat_id,card_reducere_id,sursa_model) VALUES(?,?,?,?,?,?,?,?)",id_client,c.get("nume"),c.get("prenume"),c.get("email"),c.get("telefon"),c.get("magazin_preferat_id"),c.get("card_reducere_id"),"normalizat")
        n+=1
    conn.commit()
    print(f"stg.Clienti_Normalizat inserate: {n}")
def incarca_clienti_denormalizat():
    n=0
    for c in db_denorm["clienti_denormalizat"].find():
        id_client=val(c,"id_client","cod_client","_id")
        if not id_client or exista("SELECT COUNT(*) FROM stg.Clienti_Denormalizat WHERE id_client=?",id_client):
            continue
        magazin=c.get("magazin_preferat") or {}
        card=c.get("card_reducere") or {}
        cursor.execute("INSERT INTO stg.Clienti_Denormalizat(id_client,nume,prenume,email,telefon,magazin_preferat,card_reducere,sursa_model) VALUES(?,?,?,?,?,?,?,?)",id_client,c.get("nume"),c.get("prenume"),c.get("email"),c.get("telefon"),magazin.get("denumire") if isinstance(magazin,dict) else None,card.get("cod_card") if isinstance(card,dict) else None,"denormalizat")
        n+=1
    conn.commit()
    print(f"stg.Clienti_Denormalizat inserate: {n}")
def incarca_produse_normalizat():
    n=0
    for p in db_norm["produse"].find():
        id_produs=val(p,"id_produs","cod_produs","_id")
        if not id_produs or exista("SELECT COUNT(*) FROM stg.Produse_Normalizat WHERE id_produs=?",id_produs):
            continue
        cursor.execute("INSERT INTO stg.Produse_Normalizat(id_produs,denumire,categorie_id,brand_id,pret_baza_mdl,pret_angro_mdl,sursa_model) VALUES(?,?,?,?,?,?,?)",id_produs,p.get("denumire"),p.get("categorie_id"),p.get("brand_id"),p.get("pret_baza_mdl"),p.get("pret_angro_mdl"),"normalizat")
        n+=1
    conn.commit()
    print(f"stg.Produse_Normalizat inserate: {n}")
def incarca_produse_denormalizat():
    n=0
    for p in db_denorm["produse_denormalizat"].find():
        id_produs=val(p,"id_produs","cod_produs","_id")
        if not id_produs or exista("SELECT COUNT(*) FROM stg.Produse_Denormalizat WHERE id_produs=?",id_produs):
            continue
        categorie=p.get("categorie") or {}
        brand=p.get("brand") or {}
        promotii=p.get("promotii") or []
        cursor.execute("INSERT INTO stg.Produse_Denormalizat(id_produs,denumire,categorie,brand,pret_baza_mdl,pret_angro_mdl,nr_promotii,sursa_model) VALUES(?,?,?,?,?,?,?,?)",id_produs,p.get("denumire"),categorie.get("denumire") if isinstance(categorie,dict) else None,brand.get("denumire") if isinstance(brand,dict) else None,p.get("pret_baza_mdl"),p.get("pret_angro_mdl"),len(promotii) if isinstance(promotii,list) else 0,"denormalizat")
        n+=1
    conn.commit()
    print(f"stg.Produse_Denormalizat inserate: {n}")
def incarca_comenzi_normalizat():
    n=0
    nr_linii={}
    for l in db_norm["linii_comanda"].find():
        cid=l.get("comanda_id")
        nr_linii[cid]=nr_linii.get(cid,0)+1
    for c in db_norm["comenzi"].find():
        id_comanda=val(c,"id_comanda","cod_comanda","nr_comanda","_id")
        if not id_comanda or exista("SELECT COUNT(*) FROM stg.Comenzi_Normalizat WHERE id_comanda=?",id_comanda):
            continue
        cursor.execute("INSERT INTO stg.Comenzi_Normalizat(id_comanda,id_client,id_magazin,data_comanda,total_net_mdl,nr_linii,sursa_model) VALUES(?,?,?,?,?,?,?)",id_comanda,c.get("client_id"),c.get("magazin_id"),data_sql(c.get("data_comanda")),c.get("total_net_mdl"),nr_linii.get(c.get("_id"),0),"normalizat")
        n+=1
    conn.commit()
    print(f"stg.Comenzi_Normalizat inserate: {n}")
def incarca_comenzi_denormalizat():
    n=0
    for client in db_denorm["clienti_denormalizat"].find():
        id_client=val(client,"id_client","cod_client","_id")
        for c in client.get("comenzi",[]):
            id_comanda=val(c,"id_comanda","cod_comanda","nr_comanda","_id")
            if not id_comanda or exista("SELECT COUNT(*) FROM stg.Comenzi_Denormalizat WHERE id_comanda=?",id_comanda):
                continue
            magazin=c.get("magazin") or {}
            linii=c.get("linii") or []
            cursor.execute("INSERT INTO stg.Comenzi_Denormalizat(id_comanda,id_client,magazin,data_comanda,total_net_mdl,nr_linii,sursa_model) VALUES(?,?,?,?,?,?,?)",id_comanda,id_client,magazin.get("denumire") if isinstance(magazin,dict) else None,data_sql(c.get("data_comanda")),c.get("total_net_mdl"),len(linii),"denormalizat")
            n+=1
    conn.commit()
    print(f"stg.Comenzi_Denormalizat inserate: {n}")
def incarca_companii_normalizat():
    n=0
    for c in db_norm["companii"].find():
        id_companie=val(c,"id_companie","cod_companie","_id")
        if not id_companie or exista("SELECT COUNT(*) FROM stg.Companii_Normalizat WHERE id_companie=?",id_companie):
            continue
        cursor.execute("INSERT INTO stg.Companii_Normalizat(id_companie,denumire,IDNO,tip_activitate,sursa_model) VALUES(?,?,?,?,?)",id_companie,c.get("denumire"),c.get("IDNO") or c.get("idno"),c.get("tip_activitate"),"normalizat")
        n+=1
    conn.commit()
    print(f"stg.Companii_Normalizat inserate: {n}")
def incarca_companii_denormalizat():
    n=0
    for c in db_denorm["companii_denormalizat"].find():
        id_companie=val(c,"id_companie","cod_companie","_id")
        if not id_companie or exista("SELECT COUNT(*) FROM stg.Companii_Denormalizat WHERE id_companie=?",id_companie):
            continue
        contracte=c.get("contracte") or []
        comenzi=c.get("comenzi_angro") or []
        cursor.execute("INSERT INTO stg.Companii_Denormalizat(id_companie,denumire,IDNO,tip_activitate,nr_contracte,nr_comenzi_angro,valoare_totala_comenzi_angro_mdl,sursa_model) VALUES(?,?,?,?,?,?,?,?)",id_companie,c.get("denumire"),c.get("IDNO") or c.get("idno"),c.get("tip_activitate"),len(contracte),len(comenzi),c.get("valoare_totala_comenzi_angro_mdl"),"denormalizat")
        n+=1
    conn.commit()
    print(f"stg.Companii_Denormalizat inserate: {n}")
def populeaza_dim_client():
    executa("INSERT INTO dwh.DimClient(id_client,nume,prenume,email,telefon,sursa_model) SELECT s.id_client,s.nume,s.prenume,s.email,s.telefon,'normalizat' FROM stg.Clienti_Normalizat s WHERE NOT EXISTS(SELECT 1 FROM dwh.DimClient d WHERE d.id_client=s.id_client AND d.sursa_model='normalizat')")
    executa("INSERT INTO dwh.DimClient(id_client,nume,prenume,email,telefon,sursa_model) SELECT s.id_client,s.nume,s.prenume,s.email,s.telefon,'denormalizat' FROM stg.Clienti_Denormalizat s WHERE NOT EXISTS(SELECT 1 FROM dwh.DimClient d WHERE d.id_client=s.id_client AND d.sursa_model='denormalizat')")
    print("dwh.DimClient actualizat")
def populeaza_dim_companie():
    executa("INSERT INTO dwh.DimCompanie(id_companie,denumire,IDNO,tip_activitate,sursa_model) SELECT s.id_companie,s.denumire,s.IDNO,s.tip_activitate,'normalizat' FROM stg.Companii_Normalizat s WHERE NOT EXISTS(SELECT 1 FROM dwh.DimCompanie d WHERE d.id_companie=s.id_companie AND d.sursa_model='normalizat')")
    executa("INSERT INTO dwh.DimCompanie(id_companie,denumire,IDNO,tip_activitate,sursa_model) SELECT s.id_companie,s.denumire,s.IDNO,s.tip_activitate,'denormalizat' FROM stg.Companii_Denormalizat s WHERE NOT EXISTS(SELECT 1 FROM dwh.DimCompanie d WHERE d.id_companie=s.id_companie AND d.sursa_model='denormalizat')")
    print("dwh.DimCompanie actualizat")
def populeaza_dim_produs():
    executa("INSERT INTO dwh.DimProdus(id_produs,denumire,categorie,brand,pret_baza_mdl,pret_angro_mdl,sursa_model) SELECT s.id_produs,s.denumire,s.categorie_id,s.brand_id,s.pret_baza_mdl,s.pret_angro_mdl,'normalizat' FROM stg.Produse_Normalizat s WHERE NOT EXISTS(SELECT 1 FROM dwh.DimProdus d WHERE d.id_produs=s.id_produs AND d.sursa_model='normalizat')")
    executa("INSERT INTO dwh.DimProdus(id_produs,denumire,categorie,brand,pret_baza_mdl,pret_angro_mdl,sursa_model) SELECT s.id_produs,s.denumire,s.categorie,s.brand,s.pret_baza_mdl,s.pret_angro_mdl,'denormalizat' FROM stg.Produse_Denormalizat s WHERE NOT EXISTS(SELECT 1 FROM dwh.DimProdus d WHERE d.id_produs=s.id_produs AND d.sursa_model='denormalizat')")
    print("dwh.DimProdus actualizat")
def populeaza_dim_timp():
    executa("INSERT INTO dwh.DimTimp(data_calendaristica,zi,luna,trimestru,an) SELECT DISTINCT s.data_comanda,DAY(s.data_comanda),MONTH(s.data_comanda),DATEPART(QUARTER,s.data_comanda),YEAR(s.data_comanda) FROM stg.Comenzi_Normalizat s WHERE s.data_comanda IS NOT NULL AND NOT EXISTS(SELECT 1 FROM dwh.DimTimp d WHERE d.data_calendaristica=s.data_comanda)")
    executa("INSERT INTO dwh.DimTimp(data_calendaristica,zi,luna,trimestru,an) SELECT DISTINCT s.data_comanda,DAY(s.data_comanda),MONTH(s.data_comanda),DATEPART(QUARTER,s.data_comanda),YEAR(s.data_comanda) FROM stg.Comenzi_Denormalizat s WHERE s.data_comanda IS NOT NULL AND NOT EXISTS(SELECT 1 FROM dwh.DimTimp d WHERE d.data_calendaristica=s.data_comanda)")
    print("dwh.DimTimp actualizat")
def populeaza_fact():
    executa("INSERT INTO dwh.FactVanzariComune(ClientKey,TimpKey,tip_flux,sursa_model,id_document_sursa,cantitate,valoare_bruta_mdl,valoare_neta_mdl,discount_mdl,nr_linii) SELECT dc.ClientKey,dt.TimpKey,'B2C','normalizat',s.id_comanda,1,s.total_net_mdl,s.total_net_mdl,0,s.nr_linii FROM stg.Comenzi_Normalizat s JOIN dwh.DimClient dc ON dc.id_client=s.id_client AND dc.sursa_model='normalizat' JOIN dwh.DimTimp dt ON dt.data_calendaristica=s.data_comanda WHERE NOT EXISTS(SELECT 1 FROM dwh.FactVanzariComune f WHERE f.id_document_sursa=s.id_comanda AND f.sursa_model='normalizat' AND f.tip_flux='B2C')")
    executa("INSERT INTO dwh.FactVanzariComune(ClientKey,TimpKey,tip_flux,sursa_model,id_document_sursa,cantitate,valoare_bruta_mdl,valoare_neta_mdl,discount_mdl,nr_linii) SELECT dc.ClientKey,dt.TimpKey,'B2C','denormalizat',s.id_comanda,1,s.total_net_mdl,s.total_net_mdl,0,s.nr_linii FROM stg.Comenzi_Denormalizat s JOIN dwh.DimClient dc ON dc.id_client=s.id_client AND dc.sursa_model='denormalizat' JOIN dwh.DimTimp dt ON dt.data_calendaristica=s.data_comanda WHERE NOT EXISTS(SELECT 1 FROM dwh.FactVanzariComune f WHERE f.id_document_sursa=s.id_comanda AND f.sursa_model='denormalizat' AND f.tip_flux='B2C')")
    executa("INSERT INTO dwh.FactVanzariComune(CompanieKey,tip_flux,sursa_model,id_document_sursa,cantitate,valoare_bruta_mdl,valoare_neta_mdl,discount_mdl,nr_linii) SELECT dc.CompanieKey,'B2B','denormalizat',s.id_companie,s.nr_comenzi_angro,s.valoare_totala_comenzi_angro_mdl,s.valoare_totala_comenzi_angro_mdl,0,s.nr_comenzi_angro FROM stg.Companii_Denormalizat s JOIN dwh.DimCompanie dc ON dc.id_companie=s.id_companie AND dc.sursa_model='denormalizat' WHERE NOT EXISTS(SELECT 1 FROM dwh.FactVanzariComune f WHERE f.id_document_sursa=s.id_companie AND f.sursa_model='denormalizat' AND f.tip_flux='B2B')")
    print("dwh.FactVanzariComune actualizat")
def populeaza_kpi_clienti():
    executa("INSERT INTO kpi.KPI_Clienti_B2C(sursa_model,id_client,nume_client,nr_comenzi,valoare_totala_mdl,valoare_medie_comanda_mdl) SELECT f.sursa_model,dc.id_client,CONCAT(dc.nume,' ',dc.prenume),COUNT(f.FactKey),SUM(f.valoare_neta_mdl),AVG(f.valoare_neta_mdl) FROM dwh.FactVanzariComune f JOIN dwh.DimClient dc ON dc.ClientKey=f.ClientKey WHERE f.tip_flux='B2C' GROUP BY f.sursa_model,dc.id_client,dc.nume,dc.prenume HAVING NOT EXISTS(SELECT 1 FROM kpi.KPI_Clienti_B2C k WHERE k.sursa_model=f.sursa_model AND k.id_client=dc.id_client)")
    print("kpi.KPI_Clienti_B2C actualizat")
def populeaza_kpi_companii():
    executa("INSERT INTO kpi.KPI_Companii_B2B(sursa_model,id_companie,denumire_companie,tip_activitate,nr_comenzi_angro,valoare_totala_angro_mdl,valoare_medie_comanda_angro_mdl) SELECT s.sursa_model,s.id_companie,s.denumire,s.tip_activitate,s.nr_comenzi_angro,s.valoare_totala_comenzi_angro_mdl,CAST(s.valoare_totala_comenzi_angro_mdl/NULLIF(s.nr_comenzi_angro,0) AS DECIMAL(12,2)) FROM stg.Companii_Denormalizat s WHERE NOT EXISTS(SELECT 1 FROM kpi.KPI_Companii_B2B k WHERE k.sursa_model=s.sursa_model AND k.id_companie=s.id_companie)")
    print("kpi.KPI_Companii_B2B actualizat")
def populeaza_kpi_produse():
    executa("INSERT INTO kpi.KPI_Produse_Promotii(sursa_model,nr_produse_total,nr_produse_promo,pondere_produse_promo_pct) SELECT s.sursa_model,COUNT(*),SUM(CASE WHEN s.nr_promotii>0 THEN 1 ELSE 0 END),CAST(SUM(CASE WHEN s.nr_promotii>0 THEN 1 ELSE 0 END)*100.0/COUNT(*) AS DECIMAL(8,2)) FROM stg.Produse_Denormalizat s GROUP BY s.sursa_model HAVING NOT EXISTS(SELECT 1 FROM kpi.KPI_Produse_Promotii k WHERE k.sursa_model=s.sursa_model)")
    print("kpi.KPI_Produse_Promotii actualizat")
def populeaza_kpi_integrat():
    executa("INSERT INTO kpi.KPI_Integrat(cod_kpi,denumire_kpi,categorie_kpi,sursa_model,entitate_id,entitate_denumire,valoare_kpi,unitate_masura) SELECT 'KPI_B2C_01','Valoarea medie a comenzii B2C','Clienti B2C',sursa_model,id_client,nume_client,valoare_medie_comanda_mdl,'MDL' FROM kpi.KPI_Clienti_B2C s WHERE NOT EXISTS(SELECT 1 FROM kpi.KPI_Integrat k WHERE k.cod_kpi='KPI_B2C_01' AND k.sursa_model=s.sursa_model AND k.entitate_id=s.id_client)")
    executa("INSERT INTO kpi.KPI_Integrat(cod_kpi,denumire_kpi,categorie_kpi,sursa_model,entitate_id,entitate_denumire,valoare_kpi,unitate_masura) SELECT 'KPI_B2B_01','Valoarea medie a comenzii B2B','Companii B2B',sursa_model,id_companie,denumire_companie,valoare_medie_comanda_angro_mdl,'MDL' FROM kpi.KPI_Companii_B2B s WHERE NOT EXISTS(SELECT 1 FROM kpi.KPI_Integrat k WHERE k.cod_kpi='KPI_B2B_01' AND k.sursa_model=s.sursa_model AND k.entitate_id=s.id_companie)")
    executa("INSERT INTO kpi.KPI_Integrat(cod_kpi,denumire_kpi,categorie_kpi,sursa_model,entitate_id,entitate_denumire,valoare_kpi,unitate_masura) SELECT 'KPI_PROD_01','Pondere produse cu promotii','Produse',sursa_model,NULL,'Catalog produse',pondere_produse_promo_pct,'%' FROM kpi.KPI_Produse_Promotii s WHERE NOT EXISTS(SELECT 1 FROM kpi.KPI_Integrat k WHERE k.cod_kpi='KPI_PROD_01' AND k.sursa_model=s.sursa_model)")
    print("kpi.KPI_Integrat actualizat")
def rezumat():
    tabele=["stg.Clienti_Normalizat","stg.Clienti_Denormalizat","stg.Comenzi_Normalizat","stg.Comenzi_Denormalizat","stg.Companii_Normalizat","stg.Companii_Denormalizat","stg.Produse_Normalizat","stg.Produse_Denormalizat","dwh.DimClient","dwh.DimCompanie","dwh.DimProdus","dwh.DimTimp","dwh.FactVanzariComune","kpi.KPI_Clienti_B2C","kpi.KPI_Companii_B2B","kpi.KPI_Integrat","kpi.KPI_Produse_Promotii"]
    print("Rezumat final:")
    for t in tabele:
        cursor.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"{t}: {cursor.fetchone()[0]}")
def inchide():
    try:
        cursor.close()
        conn.close()
        mongo.close()
    except:
        pass
def main():
    verifica_tabele()
    incarca_clienti_normalizat()
    incarca_clienti_denormalizat()
    incarca_produse_normalizat()
    incarca_produse_denormalizat()
    incarca_comenzi_normalizat()
    incarca_comenzi_denormalizat()
    incarca_companii_normalizat()
    incarca_companii_denormalizat()
    populeaza_dim_client()
    populeaza_dim_companie()
    populeaza_dim_produs()
    populeaza_dim_timp()
    populeaza_fact()
    populeaza_kpi_clienti()
    populeaza_kpi_companii()
    populeaza_kpi_produse()
    populeaza_kpi_integrat()
    rezumat()
    inchide()
if __name__=="__main__":
    main()