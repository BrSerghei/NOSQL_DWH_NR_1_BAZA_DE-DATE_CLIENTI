import os,sys,json,random,hashlib,time,argparse
from pathlib import Path
from datetime import datetime,timedelta
os.environ["PYSPARK_PYTHON"]=sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"]=sys.executable
os.environ["PYSPARK_PIN_THREAD"]="false"
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count as spark_count,sum as spark_sum,avg as spark_avg
try:
    import pyodbc
except Exception:
    pyodbc=None
COUNTS={"categorii":5,"branduri":10,"produse":60,"promotii":25,"magazine":5,"carduri_reducere":40,"clienti":70,"segmente_rfm":70,"comenzi":50,"linii_comanda":100,"companii":10,"contracte_angro":10,"comenzi_angro":10,"linii_comenzi_angro":35}
NORMAL_COLLECTIONS=list(COUNTS.keys())
def log(x):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {x}",flush=True)
def pas(x):
    log("="*70);log(x);log("="*70)
def durata(t,x):
    log(f"{x} finalizat in {round(time.time()-t,2)} secunde")
def args():
    p=argparse.ArgumentParser()
    p.add_argument("--mongo-uri",default="mongodb://localhost:27017")
    p.add_argument("--normal-db",default="nr1_clienti")
    p.add_argument("--denorm-db",default="nr1_denormalizat")
    p.add_argument("--sql-server",default="localhost")
    p.add_argument("--sql-database",default="DWH_Nr1_Comun")
    p.add_argument("--sql-auth",default="trusted",choices=["trusted","sql"])
    p.add_argument("--sql-user",default="")
    p.add_argument("--sql-password",default="")
    p.add_argument("--output",default="NR1_BIGDATA_OUTPUT")
    p.add_argument("--project-root",default="LAB4_BD")
    p.add_argument("--skip-mongo",action="store_true")
    p.add_argument("--skip-ssms",action="store_true")
    p.add_argument("--skip-spark",action="store_true")
    return p.parse_args()
def run_tag():
    return datetime.now().strftime("B%m%d%H%M%S")+str(random.randint(10,99))
def sid(prefix,tag,i):
    return f"{prefix}{tag[1:]}{i:03d}"[:20]
def fetch(url):
    try:
        r=requests.get(url,timeout=8,headers={"User-Agent":"Mozilla/5.0"})
        if r.status_code==200:
            return r.text
    except Exception:
        pass
    return ""
def public_data():
    urls={"about":"https://nr1.md/ro/about/","shops":"https://nr1.md/ro/shops/","booklets":"https://nr1.md/ro/booklets/","cards":"https://nr1.md/ro/cards/"}
    texts={k:fetch(v) for k,v in urls.items()}
    shops=[]
    soup=BeautifulSoup(texts.get("shops",""),"html.parser")
    raw=soup.get_text("\n")
    for line in [x.strip() for x in raw.splitlines() if x.strip()]:
        if any(s in line.lower() for s in ["chișinău","chisinau","bălți","balti","cahul","orhei"]):
            shops.append(line[:120])
    shops=shops[:5] if shops else ["bd. Moscova 21, Chișinău","bd. Ștefan cel Mare 8, Chișinău","str. Alba Iulia 75, Chișinău","str. Alecu Russo 15, Chișinău","str. Ismail 86, Chișinău"]
    booklets=[]
    soup=BeautifulSoup(texts.get("booklets",""),"html.parser")
    for a in soup.find_all("a"):
        t=a.get_text(" ",strip=True)
        if t and len(t)>3:
            booklets.append({"titlu":t[:120],"sursa_url":"https://nr1.md/ro/booklets/"})
    booklets=booklets[:30] if booklets else [{"titlu":"Catalog promoțional Nr.1","sursa_url":"https://nr1.md/ro/booklets/"} for _ in range(30)]
    brands=["Nr.1","Bun Străbun","Casa Rinaldi","Deroni","Barilla"]
    return {"shops":shops,"booklets":booklets,"brands":brands,"urls":urls}
def unique_phone(tag,i):
    n=int(hashlib.md5(f"{tag}-{i}-telefon".encode()).hexdigest()[:8],16)%1000000
    return f"+373 7{i%10} {n:06d}"
def unique_email(tag,i,nume,prenume):
    safe=(prenume+"."+nume).lower().replace("ă","a").replace("â","a").replace("î","i").replace("ș","s").replace("ş","s").replace("ț","t").replace("ţ","t")
    return f"{safe}.{tag.lower()}.{i:03d}@nr1-demo.md"
def build_dataset(project_root):
    t=time.time();pas("PAS 1: COLECTARE DATE PUBLICE SI GENERARE DATASET")
    log("Citire date publice Nr.1: about, shops, booklets")
    pub=public_data();tag=run_tag();random.seed(tag)
    log(f"Date publice detectate: branduri reale={len(pub['brands'])}, magazine={len(pub['shops'])}, cataloage={len(pub['booklets'])}")
    log(f"Identificator rulare: {tag}")
    cats_base=["Produse alimentare","Băuturi","Igienă și curățenie","Produse pentru casă","Dulciuri"]
    data={k:[] for k in NORMAL_COLLECTIONS}
    for i in range(1,COUNTS["categorii"]+1):
        _id=sid("CAT",tag,i);data["categorii"].append({"_id":_id,"id_categorie":_id,"denumire":f"{cats_base[i-1]} {tag}","descriere":"Categorie comercială utilizată în analiza Nr.1","sursa_tip":"public+generat_controlat","sursa_url":"https://nr1.md/ro/","lot_etl":tag})
    for i in range(1,COUNTS["branduri"]+1):
        _id=sid("BRD",tag,i);name=pub["brands"][(i-1)%len(pub["brands"])] if i<=len(pub["brands"])*2 else f"Brand Nr.1 {i}"
        data["branduri"].append({"_id":_id,"id_brand":_id,"denumire":f"{name} {tag}-{i}","tara_origine":"Republica Moldova","sursa_tip":"public+generat_controlat","sursa_url":"https://nr1.md/ro/about/","lot_etl":tag})
    for i in range(1,COUNTS["produse"]+1):
        _id=sid("PRD",tag,i);cat=data["categorii"][(i-1)%len(data["categorii"])];br=data["branduri"][(i-1)%len(data["branduri"])]
        pret=round(8+random.random()*180,2);data["produse"].append({"_id":_id,"id_produs":_id,"cod_produs":_id,"denumire":f"Produs Nr.1 {tag}-{i:03d}","categorie_id":cat["_id"],"brand_id":br["_id"],"pret_baza_mdl":pret,"pret_angro_mdl":round(pret*0.9,2),"unitate_masura":"buc","activ":True,"sursa_tip":"generat_controlat","sursa_url":"https://nr1.md/ro/","lot_etl":tag})
    for i in range(1,COUNTS["promotii"]+1):
        _id=sid("PRO",tag,i);p=data["produse"][(i-1)%len(data["produse"])];b=pub["booklets"][(i-1)%len(pub["booklets"])]
        data["promotii"].append({"_id":_id,"id_promotie":_id,"produs_id":p["_id"],"titlu":f"{b.get('titlu','Promoție Nr.1')} {tag}-{i}","tip_promotie":"reducere procentuala","discount_pct":5+i%25,"data_start":"2026-05-01","data_sfarsit":"2026-06-30","activa":True,"sursa_tip":"public+generat_controlat","sursa_url":b.get("sursa_url","https://nr1.md/ro/booklets/"),"lot_etl":tag})
    for i in range(1,COUNTS["magazine"]+1):
        _id=sid("MAG",tag,i);adr=pub["shops"][(i-1)%len(pub["shops"])]
        data["magazine"].append({"_id":_id,"id_magazin":_id,"cod_magazin":_id,"denumire":f"Magazin Nr.1 {tag}-{i}","adresa":adr,"localitate":"Chișinău","program":"08:00-22:00","sursa_tip":"public+generat_controlat","sursa_url":"https://nr1.md/ro/shops/","lot_etl":tag})
    for i in range(1,COUNTS["carduri_reducere"]+1):
        _id=sid("CRD",tag,i);data["carduri_reducere"].append({"_id":_id,"id_card":_id,"cod_card":f"CARD-{tag}-{i:03d}","tip_card":"reducere Nr.1","discount_maxim_pct":5,"data_emitere":"2023-01-01","activ":True,"sursa_tip":"public+generat_controlat","sursa_url":"https://nr1.md/ro/cards/","lot_etl":tag})
    nume=["Popescu","Rusu","Ceban","Balan","Munteanu","Moraru","Toma","Țurcan","Ursu","Cojocaru","Sandu","Lungu","Botezatu","Guțu"]
    pren=["Ana","Ion","Maria","Sergiu","Alina","Victor","Olga","Andrei","Irina","Radu","Petru","Nina","Maxim","Elena"]
    for i in range(1,COUNTS["clienti"]+1):
        _id=sid("CLI",tag,i);n=nume[(i-1)%len(nume)];pr=pren[(i*3)%len(pren)];card=data["carduri_reducere"][(i-1)%len(data["carduri_reducere"])] if i<=COUNTS["carduri_reducere"] else None;mag=data["magazine"][(i-1)%len(data["magazine"])]
        data["clienti"].append({"_id":_id,"id_client":_id,"cod_client":_id,"nume":n,"prenume":pr,"telefon":unique_phone(tag,i),"email":unique_email(tag,i,n,pr),"data_inregistrare":str((datetime(2021,1,1)+timedelta(days=i*17%1500)).date()),"card_reducere_id":card["_id"] if card else None,"magazin_preferat_id":mag["_id"],"preferinte_categorii":[data["categorii"][i%5]["_id"],data["categorii"][(i+2)%5]["_id"]],"activ":True,"sursa_tip":"generat_controlat","lot_etl":tag})
    for i,c in enumerate(data["clienti"],1):
        _id=sid("RFM",tag,i);rec=random.randint(1,5);freq=random.randint(1,5);mon=random.randint(1,5)
        data["segmente_rfm"].append({"_id":_id,"id_segment":_id,"client_id":c["_id"],"recency_score":rec,"frequency_score":freq,"monetary_score":mon,"scor_total":rec+freq+mon,"segment":"client activ" if rec+freq+mon>=10 else "client ocazional","data_calcul":"2026-05-23","sursa_tip":"generat_controlat","lot_etl":tag})
    for i in range(1,COUNTS["comenzi"]+1):
        _id=sid("COM",tag,i);cl=data["clienti"][(i-1)%len(data["clienti"])];mag=data["magazine"][(i-1)%len(data["magazine"])]
        data["comenzi"].append({"_id":_id,"id_comanda":_id,"nr_comanda":f"BON-{tag}-{i:03d}","client_id":cl["_id"],"magazin_id":mag["_id"],"data_comanda":str((datetime(2026,1,1)+timedelta(days=i%120)).date()),"total_brut_mdl":0,"discount_mdl":0,"total_net_mdl":0,"metoda_plata":"card","sursa_tip":"generat_controlat","lot_etl":tag})
    line_no=1
    for com in data["comenzi"]:
        total=0
        for j in range(2):
            p=data["produse"][(line_no+j)%len(data["produse"])];cant=1+(line_no+j)%4;pret=p["pret_baza_mdl"];val=round(cant*pret,2);disc=round(val*0.03,2);net=round(val-disc,2);_id=sid("LCM",tag,line_no)
            data["linii_comanda"].append({"_id":_id,"id_linie":_id,"comanda_id":com["_id"],"produs_id":p["_id"],"cantitate":cant,"pret_unitar_mdl":pret,"valoare_bruta_mdl":val,"discount_mdl":disc,"valoare_neta_mdl":net,"sursa_tip":"generat_controlat","lot_etl":tag});total+=net;line_no+=1
        com["total_brut_mdl"]=round(total/0.97,2);com["discount_mdl"]=round(com["total_brut_mdl"]-total,2);com["total_net_mdl"]=round(total,2)
    tipuri=["restaurant","hotel","cafenea","catering","centru de afaceri"]
    for i in range(1,COUNTS["companii"]+1):
        _id=sid("CMP",tag,i);data["companii"].append({"_id":_id,"id_companie":_id,"cod_companie":_id,"denumire":f"Companie Partener Nr.1 {tag}-{i}","IDNO":f"{1000000000000+i+int(tag[-4:])}","idno":f"{1000000000000+i+int(tag[-4:])}","tip_activitate":tipuri[(i-1)%len(tipuri)],"adresa_livrare":f"str. Industrială {i}, Chișinău","persoana_contact":f"Manager {i}","telefon":f"+373 22 {int(tag[-4:])+i:06d}"[:15],"email":f"b2b_{tag.lower()}_{i:03d}@nr1-demo.md","tva_inregistrat":True,"sursa_tip":"generat_controlat","sursa_url":"https://nr1.md/ro/for-business/wholesalers/","lot_etl":tag})
    for i,c in enumerate(data["companii"],1):
        _id=sid("CTR",tag,i);data["contracte_angro"].append({"_id":_id,"id_contract":_id,"nr_contract":f"CTR-{tag}-{i:03d}","cod_contract":f"CTR-{tag}-{i:03d}","companie_id":c["_id"],"data_start":"2026-01-01","data_sfarsit":"2026-12-31","discount_contractual_pct":5+i%6,"termen_plata_zile":7*(1+i%4),"limita_credit_mdl":50000+i*10000,"status":"activ","moneda":"MDL","sursa_tip":"generat_controlat","lot_etl":tag})
    for i,c in enumerate(data["companii"],1):
        _id=sid("CAG",tag,i);data["comenzi_angro"].append({"_id":_id,"id_comanda_angro":_id,"nr_comanda_angro":f"ANG-{tag}-{i:03d}","companie_id":c["_id"],"contract_id":data["contracte_angro"][i-1]["_id"],"data_comanda":"2026-05-10","status":"livrat","valoare_bruta_mdl":0,"discount_mdl":0,"valoare_neta_mdl":0,"moneda":"MDL","sursa_tip":"generat_controlat","lot_etl":tag})
    line_no=1
    n_ord=len(data["comenzi_angro"])
    per=COUNTS["linii_comenzi_angro"]//n_ord
    xtr=COUNTS["linii_comenzi_angro"]%n_ord
    for idx,com in enumerate(data["comenzi_angro"]):
        total=0
        for j in range(per+(1 if idx<xtr else 0)):
            if line_no>COUNTS["linii_comenzi_angro"]:break
            p=data["produse"][(line_no+j)%len(data["produse"])];cant=10+(line_no+j)%20;pret=p["pret_angro_mdl"];val=round(cant*pret,2);disc=round(val*0.07,2);net=round(val-disc,2);_id=sid("LAG",tag,line_no)
            data["linii_comenzi_angro"].append({"_id":_id,"id_linie_angro":_id,"comanda_angro_id":com["_id"],"produs_id":p["_id"],"cantitate":cant,"pret_unitar_mdl":pret,"valoare_bruta_mdl":val,"discount_mdl":disc,"valoare_neta_mdl":net,"sursa_tip":"generat_controlat","lot_etl":tag});total+=net;line_no+=1
        com["valoare_bruta_mdl"]=round(total/0.93,2);com["discount_mdl"]=round(com["valoare_bruta_mdl"]-total,2);com["valoare_neta_mdl"]=round(total,2)
    validate(data)
    denorm=build_denorm(data,tag)
    manifest={"run_id":tag,"total_documente_normalizate":sum(len(data[k]) for k in NORMAL_COLLECTIONS),"counts":{k:len(v) for k,v in data.items()},"sursa":"Nr.1 public + generat controlat"}
    durata(t,"Construire dataset Big Data")
    return data,denorm,manifest
def validate(data):
    errors=[];total=sum(len(data[k]) for k in NORMAL_COLLECTIONS)
    if total!=500:errors.append(f"total invalid {total}")
    for k,docs in data.items():
        ids=[d.get("_id") for d in docs]
        if len(ids)!=len(set(ids)):errors.append(f"duplicate _id in {k}")
        if any(not x for x in ids):errors.append(f"missing _id in {k}")
    if errors:raise ValueError("; ".join(errors))
    log(f"Validare reusita: total documente normalizate={total}")
def build_denorm(data,tag):
    produse={p["_id"]:p for p in data["produse"]};cats={c["_id"]:c for c in data["categorii"]};brands={b["_id"]:b for b in data["branduri"]};mags={m["_id"]:m for m in data["magazine"]};cards={c["_id"]:c for c in data["carduri_reducere"]};clients={c["_id"]:c for c in data["clienti"]};comp={c["_id"]:c for c in data["companii"]}
    linii_by_com={}
    for l in data["linii_comanda"]:linii_by_com.setdefault(l["comanda_id"],[]).append(l)
    comenzi_by_client={}
    for c in data["comenzi"]:
        cc=dict(c);cc["magazin"]=mags.get(c["magazin_id"],{});cc["linii"]=[{**l,"produs":produse.get(l["produs_id"],{})} for l in linii_by_com.get(c["_id"],[])];comenzi_by_client.setdefault(c["client_id"],[]).append(cc)
    clienti_den=[]
    for c in data["clienti"]:
        d=dict(c);d["magazin_preferat"]=mags.get(c.get("magazin_preferat_id"),{});d["card_reducere"]=cards.get(c.get("card_reducere_id"),{});d["comenzi"]=comenzi_by_client.get(c["_id"],[]);clienti_den.append(d)
    produse_den=[]
    promos={}
    for pr in data["promotii"]:promos.setdefault(pr["produs_id"],[]).append(pr)
    for p in data["produse"]:
        d=dict(p);d["categorie"]=cats.get(p["categorie_id"],{});d["brand"]=brands.get(p["brand_id"],{});d["promotii"]=promos.get(p["_id"],[]);produse_den.append(d)
    linii_angro={}
    for l in data["linii_comenzi_angro"]:linii_angro.setdefault(l["comanda_angro_id"],[]).append(l)
    contracte_by_comp={}
    for c in data["contracte_angro"]:contracte_by_comp.setdefault(c["companie_id"],[]).append(c)
    comenzi_angro_by_comp={}
    for c in data["comenzi_angro"]:
        cc=dict(c);cc["linii"]=[{**l,"produs":produse.get(l["produs_id"],{})} for l in linii_angro.get(c["_id"],[])];comenzi_angro_by_comp.setdefault(c["companie_id"],[]).append(cc)
    comp_den=[]
    for c in data["companii"]:
        d=dict(c);d["contracte"]=contracte_by_comp.get(c["_id"],[]);d["comenzi_angro"]=comenzi_angro_by_comp.get(c["_id"],[]);d["valoare_totala_comenzi_angro_mdl"]=round(sum(x.get("valoare_neta_mdl",0) for x in d["comenzi_angro"]),2);comp_den.append(d)
    return {"clienti_denormalizat":clienti_den,"companii_denormalizat":comp_den,"produse_denormalizat":produse_den,"nr1_denormalizat_full":[{"_id":"FULL"+tag[1:]+"001","id_model":"FULL"+tag[1:]+"001","clienti":clienti_den,"companii":comp_den,"produse":produse_den,"lot_etl":tag}]}
def save_outputs(output,data,denorm,manifest):
    t=time.time();pas("PAS 2: SALVARE JSON IN MAPA COMUNA")
    base=Path(output);nbase=base/"JSON_IMPORT_NORMALIZAT_BIGDATA";dbase=base/"JSON_IMPORT_DENORMALIZAT_BIGDATA";nbase.mkdir(parents=True,exist_ok=True);dbase.mkdir(parents=True,exist_ok=True)
    for k,v in data.items():
        p=nbase/f"{k}.json";p.write_text(json.dumps(v,ensure_ascii=False,indent=2,default=str),encoding="utf-8");log(f"JSON normalizat salvat: {k}.json | documente: {len(v)}")
    for k,v in denorm.items():
        p=dbase/f"{k}.json";p.write_text(json.dumps(v,ensure_ascii=False,indent=2,default=str),encoding="utf-8");log(f"JSON denormalizat salvat: {k}.json | documente: {len(v)}")
    (base/"manifest_sursee_si_limite.json").write_text(json.dumps(manifest,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    log("Manifestul surselor si limitelor a fost salvat");durata(t,"Salvare fisiere")
def normalize_for_spark(d,collection):
    r={"colectie":collection}
    for k,v in d.items():
        if v is None:r[k]=""
        elif isinstance(v,(dict,list)):r[k]=json.dumps(v,ensure_ascii=False,default=str)
        else:r[k]=str(v)
    return r
def spark_process(output,data,denorm):
    t=time.time();pas("PAS 3: PROCESARE ANALITICA CU SPARK SI SALVARE JSONL")
    log("Pornire modul Spark pentru procesare analitica locala")
    spark=SparkSession.builder.master("local[1]").appName("NR1_BigData_ETL").config("spark.pyspark.python",sys.executable).config("spark.pyspark.driver.python",sys.executable).config("spark.driver.host","127.0.0.1").config("spark.driver.bindAddress","127.0.0.1").config("spark.sql.shuffle.partitions","1").config("spark.default.parallelism","1").config("spark.python.worker.reuse","false").config("spark.network.timeout","600s").config("spark.executor.heartbeatInterval","60s").config("spark.ui.enabled","false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    base=Path(output)/"ANALYTIC_JSONL";base.mkdir(parents=True,exist_ok=True)
    rows=[];collections={}
    for k,v in data.items():collections[k]=v
    for k,v in denorm.items():collections[k]=v
    for k,v in collections.items():
        log(f"Spark: pregatire randuri pentru {k} | documente: {len(v)}")
        rows.extend([normalize_for_spark(d,k) for d in v])
    log(f"Spark: creare DataFrame unic pentru {len(rows)} randuri analitice")
    df=spark.createDataFrame(rows)
    stats=df.groupBy("colectie").agg(spark_count("*").alias("nr_documente")).collect()
    all_json=df.toJSON().collect()
    grouped={k:[] for k in collections.keys()}
    for s in all_json:
        obj=json.loads(s);grouped[obj.get("colectie","")].append(s)
    for k,arr in grouped.items():
        with open(base/f"{k}.jsonl","w",encoding="utf-8") as f:
            for line in arr:f.write(line+"\n")
        log(f"Spark: {k} procesat si salvat JSONL | documente: {len(arr)}")
    with open(base/"statistici_bigdata_spark.jsonl","w",encoding="utf-8") as f:
        for r in stats:f.write(json.dumps({"colectie":r["colectie"],"nr_documente":r["nr_documente"]},ensure_ascii=False)+"\n")
    analize=[]
    pdf=spark.createDataFrame([{"categorie_id":p["categorie_id"],"pret_baza_mdl":float(p["pret_baza_mdl"])} for p in data["produse"]])
    for r in pdf.groupBy("categorie_id").agg(spark_count("*").alias("nr_produse"),spark_avg("pret_baza_mdl").alias("pret_mediu")).collect():analize.append({"analiza":"produse_pe_categorii","categorie_id":r["categorie_id"],"nr_produse":r["nr_produse"],"pret_mediu":round(r["pret_mediu"],2)})
    with open(base/"analize_spark.jsonl","w",encoding="utf-8") as f:
        for a in analize:f.write(json.dumps(a,ensure_ascii=False)+"\n")
    spark.stop();durata(t,"Procesare analitica Spark")
def strip_for_mongo(d):
    return json.loads(json.dumps(d,ensure_ascii=False,default=str))
def unique_fields_for_collection(name,d):
    fields=["_id"]
    if name=="clienti":fields+=["telefon","email","cod_client","id_client"]
    if name=="carduri_reducere":fields+=["cod_card","id_card"]
    if name=="companii":fields+=["IDNO","idno","cod_companie","id_companie"]
    if name=="contracte_angro":fields+=["nr_contract","cod_contract","id_contract"]
    if name in ["comenzi","comenzi_angro"]:fields+=["nr_comanda","nr_comanda_angro","id_comanda","id_comanda_angro"]
    if name in ["produse","branduri","categorii","magazine","promotii"]:fields+=["cod_produs","id_produs","id_brand","id_categorie","cod_magazin","id_magazin","id_promotie"]
    return [{f:d.get(f)} for f in fields if d.get(f) is not None]
def insert_collection_safe(db,name,docs):
    inserted=updated=skipped=0
    for doc in docs:
        d=strip_for_mongo(doc)
        try:
            existing=db[name].find_one({"_id":d["_id"]})
            if existing:
                db[name].update_one({"_id":d["_id"]},{"$set":d});updated+=1;continue
            conflict=False
            for q in unique_fields_for_collection(name,d):
                if db[name].find_one(q,{"_id":1}):conflict=True;break
            if conflict:
                skipped+=1;continue
            db[name].insert_one(d);inserted+=1
        except DuplicateKeyError:
            skipped+=1
    log(f"MongoDB {db.name}.{name}: inserate {inserted}, actualizate {updated}, omise {skipped}, total procesate {len(docs)}")
    return {"inserted":inserted,"updated":updated,"skipped":skipped,"processed":len(docs)}
def insert_mongo(uri,normal_db,denorm_db,data,denorm):
    t=time.time();pas("PAS 4: INSERARE MONGODB IN BAZELE EXISTENTE")
    client=MongoClient(uri,serverSelectionTimeoutMS=5000);client.admin.command("ping")
    log(f"Conectare MongoDB reusita");db=client[normal_db];ddb=client[denorm_db]
    for k,v in data.items():insert_collection_safe(db,k,v)
    for k,v in denorm.items():insert_collection_safe(ddb,k,v)
    client.close();durata(t,"Inserare MongoDB in bazele existente")
def trunc(x,n):
    if x is None:return None
    return str(x)[:n]
def table_exists(cur,s,t):
    cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",s,t);return cur.fetchone()[0]>0
def ex(cur,sql,*p):
    cur.execute(sql,p);return cur.fetchone()[0]>0
def connect_sql(server,database,trusted,user,password):
    if pyodbc is None:raise RuntimeError("pyodbc nu este instalat")
    if trusted:
        return pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};"+f"SERVER={server};DATABASE={database};Trusted_Connection=yes;")
    return pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};"+f"SERVER={server};DATABASE={database};UID={user};PWD={password};")
def insert_ssms(server,database,trusted,user,password,data,denorm,output):
    t=time.time();pas("PAS 5: INSERARE SSMS IN DWH/KPI EXISTENT")
    conn=connect_sql(server,database,trusted,user,password);cur=conn.cursor()
    tables=[("stg","Clienti_Normalizat"),("stg","Clienti_Denormalizat"),("stg","Produse_Normalizat"),("stg","Produse_Denormalizat"),("stg","Comenzi_Normalizat"),("stg","Comenzi_Denormalizat"),("stg","Companii_Normalizat"),("stg","Companii_Denormalizat"),("dwh","DimClient"),("dwh","DimProdus"),("dwh","DimCompanie"),("dwh","DimTimp"),("dwh","FactVanzariComune"),("kpi","KPI_Clienti_B2C"),("kpi","KPI_Produse_Promotii"),("kpi","KPI_Companii_B2B"),("kpi","KPI_Integrat")]
    missing=[f"{s}.{tb}" for s,tb in tables if not table_exists(cur,s,tb)]
    if missing:raise RuntimeError("Lipsesc tabele SSMS: "+", ".join(missing))
    counts={}
    n=0
    for c in data["clienti"]:
        idc=trunc(c["_id"],20)
        if ex(cur,"SELECT COUNT(*) FROM stg.Clienti_Normalizat WHERE id_client=?",idc):continue
        cur.execute("INSERT INTO stg.Clienti_Normalizat(id_client,nume,prenume,email,telefon,magazin_preferat_id,card_reducere_id,sursa_model) VALUES(?,?,?,?,?,?,?,?)",idc,trunc(c.get("nume"),100),trunc(c.get("prenume"),100),trunc(c.get("email"),200),trunc(c.get("telefon"),30),trunc(c.get("magazin_preferat_id"),20),trunc(c.get("card_reducere_id"),20),"normalizat_bigdata");n+=1
    counts["stg.Clienti_Normalizat"]=n
    n=0
    for c in denorm["clienti_denormalizat"]:
        idc=trunc(c["_id"],20)
        if ex(cur,"SELECT COUNT(*) FROM stg.Clienti_Denormalizat WHERE id_client=?",idc):continue
        cur.execute("INSERT INTO stg.Clienti_Denormalizat(id_client,nume,prenume,email,telefon,magazin_preferat,card_reducere,sursa_model) VALUES(?,?,?,?,?,?,?,?)",idc,trunc(c.get("nume"),100),trunc(c.get("prenume"),100),trunc(c.get("email"),200),trunc(c.get("telefon"),30),trunc((c.get("magazin_preferat") or {}).get("denumire"),200),trunc((c.get("card_reducere") or {}).get("cod_card"),100),"denormalizat_bigdata");n+=1
    counts["stg.Clienti_Denormalizat"]=n
    n=0
    for p in data["produse"]:
        pid=trunc(p["_id"],20)
        if ex(cur,"SELECT COUNT(*) FROM stg.Produse_Normalizat WHERE id_produs=?",pid):continue
        cur.execute("INSERT INTO stg.Produse_Normalizat(id_produs,denumire,categorie_id,brand_id,pret_baza_mdl,pret_angro_mdl,sursa_model) VALUES(?,?,?,?,?,?,?)",pid,trunc(p.get("denumire"),220),trunc(p.get("categorie_id"),20),trunc(p.get("brand_id"),20),p.get("pret_baza_mdl"),p.get("pret_angro_mdl"),"normalizat_bigdata");n+=1
    counts["stg.Produse_Normalizat"]=n
    n=0
    for p in denorm["produse_denormalizat"]:
        pid=trunc(p["_id"],20)
        if ex(cur,"SELECT COUNT(*) FROM stg.Produse_Denormalizat WHERE id_produs=?",pid):continue
        cur.execute("INSERT INTO stg.Produse_Denormalizat(id_produs,denumire,categorie,brand,pret_baza_mdl,pret_angro_mdl,nr_promotii,sursa_model) VALUES(?,?,?,?,?,?,?,?)",pid,trunc(p.get("denumire"),220),trunc((p.get("categorie") or {}).get("denumire"),150),trunc((p.get("brand") or {}).get("denumire"),150),p.get("pret_baza_mdl"),p.get("pret_angro_mdl"),len(p.get("promotii",[])),"denormalizat_bigdata");n+=1
    counts["stg.Produse_Denormalizat"]=n
    nr_linii={}
    for l in data["linii_comanda"]:nr_linii[l["comanda_id"]]=nr_linii.get(l["comanda_id"],0)+1
    n=0
    for c in data["comenzi"]:
        cid=trunc(c["_id"],20)
        if ex(cur,"SELECT COUNT(*) FROM stg.Comenzi_Normalizat WHERE id_comanda=?",cid):continue
        cur.execute("INSERT INTO stg.Comenzi_Normalizat(id_comanda,id_client,id_magazin,data_comanda,total_net_mdl,nr_linii,sursa_model) VALUES(?,?,?,?,?,?,?)",cid,trunc(c.get("client_id"),20),trunc(c.get("magazin_id"),20),c.get("data_comanda"),c.get("total_net_mdl"),nr_linii.get(c["_id"],0),"normalizat_bigdata");n+=1
    counts["stg.Comenzi_Normalizat"]=n
    n=0
    for cli in denorm["clienti_denormalizat"]:
        for c in cli.get("comenzi",[]):
            cid=trunc(c["_id"],20)
            if ex(cur,"SELECT COUNT(*) FROM stg.Comenzi_Denormalizat WHERE id_comanda=?",cid):continue
            cur.execute("INSERT INTO stg.Comenzi_Denormalizat(id_comanda,id_client,magazin,data_comanda,total_net_mdl,nr_linii,sursa_model) VALUES(?,?,?,?,?,?,?)",cid,trunc(cli["_id"],20),trunc((c.get("magazin") or {}).get("denumire"),200),c.get("data_comanda"),c.get("total_net_mdl"),len(c.get("linii",[])),"denormalizat_bigdata");n+=1
    counts["stg.Comenzi_Denormalizat"]=n
    n=0
    for c in data["companii"]:
        cid=trunc(c["_id"],20)
        if ex(cur,"SELECT COUNT(*) FROM stg.Companii_Normalizat WHERE id_companie=?",cid):continue
        cur.execute("INSERT INTO stg.Companii_Normalizat(id_companie,denumire,IDNO,tip_activitate,sursa_model) VALUES(?,?,?,?,?)",cid,trunc(c.get("denumire"),200),trunc(c.get("IDNO"),30),trunc(c.get("tip_activitate"),100),"normalizat_bigdata");n+=1
    counts["stg.Companii_Normalizat"]=n
    n=0
    for c in denorm["companii_denormalizat"]:
        cid=trunc(c["_id"],20)
        if ex(cur,"SELECT COUNT(*) FROM stg.Companii_Denormalizat WHERE id_companie=?",cid):continue
        cur.execute("INSERT INTO stg.Companii_Denormalizat(id_companie,denumire,IDNO,tip_activitate,nr_contracte,nr_comenzi_angro,valoare_totala_comenzi_angro_mdl,sursa_model) VALUES(?,?,?,?,?,?,?,?)",cid,trunc(c.get("denumire"),200),trunc(c.get("IDNO"),30),trunc(c.get("tip_activitate"),100),len(c.get("contracte",[])),len(c.get("comenzi_angro",[])),c.get("valoare_totala_comenzi_angro_mdl",0),"denormalizat_bigdata");n+=1
    counts["stg.Companii_Denormalizat"]=n
    conn.commit()
    sqls=["INSERT INTO dwh.DimClient(id_client,nume,prenume,email,telefon,sursa_model) SELECT s.id_client,s.nume,s.prenume,s.email,s.telefon,s.sursa_model FROM stg.Clienti_Normalizat s WHERE NOT EXISTS(SELECT 1 FROM dwh.DimClient d WHERE d.id_client=s.id_client AND d.sursa_model=s.sursa_model)","INSERT INTO dwh.DimClient(id_client,nume,prenume,email,telefon,sursa_model) SELECT s.id_client,s.nume,s.prenume,s.email,s.telefon,s.sursa_model FROM stg.Clienti_Denormalizat s WHERE NOT EXISTS(SELECT 1 FROM dwh.DimClient d WHERE d.id_client=s.id_client AND d.sursa_model=s.sursa_model)","INSERT INTO dwh.DimProdus(id_produs,denumire,categorie,brand,pret_baza_mdl,pret_angro_mdl,sursa_model) SELECT s.id_produs,s.denumire,s.categorie_id,s.brand_id,s.pret_baza_mdl,s.pret_angro_mdl,s.sursa_model FROM stg.Produse_Normalizat s WHERE NOT EXISTS(SELECT 1 FROM dwh.DimProdus d WHERE d.id_produs=s.id_produs AND d.sursa_model=s.sursa_model)","INSERT INTO dwh.DimProdus(id_produs,denumire,categorie,brand,pret_baza_mdl,pret_angro_mdl,sursa_model) SELECT s.id_produs,s.denumire,s.categorie,s.brand,s.pret_baza_mdl,s.pret_angro_mdl,s.sursa_model FROM stg.Produse_Denormalizat s WHERE NOT EXISTS(SELECT 1 FROM dwh.DimProdus d WHERE d.id_produs=s.id_produs AND d.sursa_model=s.sursa_model)","INSERT INTO dwh.DimCompanie(id_companie,denumire,IDNO,tip_activitate,sursa_model) SELECT s.id_companie,s.denumire,s.IDNO,s.tip_activitate,s.sursa_model FROM stg.Companii_Normalizat s WHERE NOT EXISTS(SELECT 1 FROM dwh.DimCompanie d WHERE d.id_companie=s.id_companie AND d.sursa_model=s.sursa_model)","INSERT INTO dwh.DimCompanie(id_companie,denumire,IDNO,tip_activitate,sursa_model) SELECT s.id_companie,s.denumire,s.IDNO,s.tip_activitate,s.sursa_model FROM stg.Companii_Denormalizat s WHERE NOT EXISTS(SELECT 1 FROM dwh.DimCompanie d WHERE d.id_companie=s.id_companie AND d.sursa_model=s.sursa_model)","INSERT INTO dwh.DimTimp(data_calendaristica,zi,luna,trimestru,an) SELECT DISTINCT s.data_comanda,DAY(s.data_comanda),MONTH(s.data_comanda),DATEPART(QUARTER,s.data_comanda),YEAR(s.data_comanda) FROM stg.Comenzi_Normalizat s WHERE s.data_comanda IS NOT NULL AND NOT EXISTS(SELECT 1 FROM dwh.DimTimp d WHERE d.data_calendaristica=s.data_comanda)","INSERT INTO dwh.DimTimp(data_calendaristica,zi,luna,trimestru,an) SELECT DISTINCT s.data_comanda,DAY(s.data_comanda),MONTH(s.data_comanda),DATEPART(QUARTER,s.data_comanda),YEAR(s.data_comanda) FROM stg.Comenzi_Denormalizat s WHERE s.data_comanda IS NOT NULL AND NOT EXISTS(SELECT 1 FROM dwh.DimTimp d WHERE d.data_calendaristica=s.data_comanda)","INSERT INTO dwh.FactVanzariComune(ClientKey,TimpKey,tip_flux,sursa_model,id_document_sursa,cantitate,valoare_bruta_mdl,valoare_neta_mdl,discount_mdl,nr_linii) SELECT dc.ClientKey,dt.TimpKey,'B2C',s.sursa_model,s.id_comanda,1,s.total_net_mdl,s.total_net_mdl,0,s.nr_linii FROM stg.Comenzi_Normalizat s JOIN dwh.DimClient dc ON dc.id_client=s.id_client AND dc.sursa_model=s.sursa_model JOIN dwh.DimTimp dt ON dt.data_calendaristica=s.data_comanda WHERE NOT EXISTS(SELECT 1 FROM dwh.FactVanzariComune f WHERE f.id_document_sursa=s.id_comanda AND f.sursa_model=s.sursa_model)","INSERT INTO dwh.FactVanzariComune(ClientKey,TimpKey,tip_flux,sursa_model,id_document_sursa,cantitate,valoare_bruta_mdl,valoare_neta_mdl,discount_mdl,nr_linii) SELECT dc.ClientKey,dt.TimpKey,'B2C',s.sursa_model,s.id_comanda,1,s.total_net_mdl,s.total_net_mdl,0,s.nr_linii FROM stg.Comenzi_Denormalizat s JOIN dwh.DimClient dc ON dc.id_client=s.id_client AND dc.sursa_model=s.sursa_model JOIN dwh.DimTimp dt ON dt.data_calendaristica=s.data_comanda WHERE NOT EXISTS(SELECT 1 FROM dwh.FactVanzariComune f WHERE f.id_document_sursa=s.id_comanda AND f.sursa_model=s.sursa_model)","INSERT INTO dwh.FactVanzariComune(CompanieKey,tip_flux,sursa_model,id_document_sursa,cantitate,valoare_bruta_mdl,valoare_neta_mdl,discount_mdl,nr_linii) SELECT dc.CompanieKey,'B2B',s.sursa_model,s.id_companie,s.nr_comenzi_angro,s.valoare_totala_comenzi_angro_mdl,s.valoare_totala_comenzi_angro_mdl,0,s.nr_comenzi_angro FROM stg.Companii_Denormalizat s JOIN dwh.DimCompanie dc ON dc.id_companie=s.id_companie AND dc.sursa_model=s.sursa_model WHERE NOT EXISTS(SELECT 1 FROM dwh.FactVanzariComune f WHERE f.id_document_sursa=s.id_companie AND f.sursa_model=s.sursa_model)"]
    for s in sqls:cur.execute(s)
    conn.commit()
    kpis=["INSERT INTO kpi.KPI_Clienti_B2C(sursa_model,id_client,nume_client,nr_comenzi,valoare_totala_mdl,valoare_medie_comanda_mdl) SELECT f.sursa_model,dc.id_client,CONCAT(dc.nume,' ',dc.prenume),COUNT(f.FactKey),SUM(f.valoare_neta_mdl),AVG(f.valoare_neta_mdl) FROM dwh.FactVanzariComune f JOIN dwh.DimClient dc ON dc.ClientKey=f.ClientKey WHERE f.tip_flux='B2C' GROUP BY f.sursa_model,dc.id_client,dc.nume,dc.prenume HAVING NOT EXISTS(SELECT 1 FROM kpi.KPI_Clienti_B2C k WHERE k.sursa_model=f.sursa_model AND k.id_client=dc.id_client)","INSERT INTO kpi.KPI_Produse_Promotii(sursa_model,nr_produse_total,nr_produse_promo,pondere_produse_promo_pct) SELECT s.sursa_model,COUNT(*),SUM(CASE WHEN s.nr_promotii>0 THEN 1 ELSE 0 END),CAST(SUM(CASE WHEN s.nr_promotii>0 THEN 1 ELSE 0 END)*100.0/COUNT(*) AS DECIMAL(8,2)) FROM stg.Produse_Denormalizat s GROUP BY s.sursa_model HAVING NOT EXISTS(SELECT 1 FROM kpi.KPI_Produse_Promotii k WHERE k.sursa_model=s.sursa_model)","INSERT INTO kpi.KPI_Companii_B2B(sursa_model,id_companie,denumire_companie,tip_activitate,nr_comenzi_angro,valoare_totala_angro_mdl,valoare_medie_comanda_angro_mdl) SELECT s.sursa_model,s.id_companie,s.denumire,s.tip_activitate,s.nr_comenzi_angro,s.valoare_totala_comenzi_angro_mdl,CAST(s.valoare_totala_comenzi_angro_mdl/NULLIF(s.nr_comenzi_angro,0) AS DECIMAL(12,2)) FROM stg.Companii_Denormalizat s WHERE NOT EXISTS(SELECT 1 FROM kpi.KPI_Companii_B2B k WHERE k.sursa_model=s.sursa_model AND k.id_companie=s.id_companie)","INSERT INTO kpi.KPI_Integrat(cod_kpi,denumire_kpi,categorie_kpi,sursa_model,entitate_id,entitate_denumire,valoare_kpi,unitate_masura) SELECT 'KPI_B2C_01','Valoarea medie a comenzii B2C','Clienti B2C',sursa_model,id_client,nume_client,valoare_medie_comanda_mdl,'MDL' FROM kpi.KPI_Clienti_B2C s WHERE NOT EXISTS(SELECT 1 FROM kpi.KPI_Integrat k WHERE k.cod_kpi='KPI_B2C_01' AND k.sursa_model=s.sursa_model AND k.entitate_id=s.id_client)","INSERT INTO kpi.KPI_Integrat(cod_kpi,denumire_kpi,categorie_kpi,sursa_model,entitate_id,entitate_denumire,valoare_kpi,unitate_masura) SELECT 'KPI_PROD_01','Pondere produse cu promotii','Produse',sursa_model,NULL,'Catalog produse',pondere_produse_promo_pct,'%' FROM kpi.KPI_Produse_Promotii s WHERE NOT EXISTS(SELECT 1 FROM kpi.KPI_Integrat k WHERE k.cod_kpi='KPI_PROD_01' AND k.sursa_model=s.sursa_model)","INSERT INTO kpi.KPI_Integrat(cod_kpi,denumire_kpi,categorie_kpi,sursa_model,entitate_id,entitate_denumire,valoare_kpi,unitate_masura) SELECT 'KPI_B2B_01','Valoarea medie a comenzii B2B','Companii B2B',sursa_model,id_companie,denumire_companie,valoare_medie_comanda_angro_mdl,'MDL' FROM kpi.KPI_Companii_B2B s WHERE NOT EXISTS(SELECT 1 FROM kpi.KPI_Integrat k WHERE k.cod_kpi='KPI_B2B_01' AND k.sursa_model=s.sursa_model AND k.entitate_id=s.id_companie)"]
    for s in kpis:cur.execute(s)
    conn.commit()
    result={"staging_inserted":counts,"counts":{}}
    for tbl in ["stg.Clienti_Normalizat","stg.Clienti_Denormalizat","stg.Produse_Normalizat","stg.Produse_Denormalizat","stg.Comenzi_Normalizat","stg.Comenzi_Denormalizat","stg.Companii_Normalizat","stg.Companii_Denormalizat","dwh.DimClient","dwh.DimProdus","dwh.DimCompanie","dwh.DimTimp","dwh.FactVanzariComune","kpi.KPI_Clienti_B2C","kpi.KPI_Produse_Promotii","kpi.KPI_Companii_B2B","kpi.KPI_Integrat"]:
        cur.execute(f"SELECT COUNT(*) FROM {tbl}");result["counts"][tbl]=cur.fetchone()[0];log(f"SSMS count: {tbl} = {result['counts'][tbl]}")
    Path(output).mkdir(parents=True,exist_ok=True);(Path(output)/"ssms_rezumat_inserare.json").write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
    cur.close();conn.close();durata(t,"Inserare SSMS in DWH/KPI existent")
    return result
def main():
    total=time.time();a=args();pas("START ETL BIG DATA NR.1")
    log(f"Project root: {a.project_root}");log(f"Output: {a.output}");log(f"MongoDB activ: {not a.skip_mongo}");log(f"Spark activ: {not a.skip_spark}");log(f"SSMS activ: {not a.skip_ssms}")
    data,denorm,manifest=build_dataset(a.project_root);save_outputs(a.output,data,denorm,manifest)
    if not a.skip_spark:spark_process(a.output,data,denorm)
    if not a.skip_mongo:insert_mongo(a.mongo_uri,a.normal_db,a.denorm_db,data,denorm)
    ssms_result=None
    if not a.skip_ssms:ssms_result=insert_ssms(a.sql_server,a.sql_database,a.sql_auth=="trusted",a.sql_user,a.sql_password,data,denorm,a.output)
    pas("FINALIZARE ETL BIG DATA NR.1");durata(total,"Proces complet")
    print(json.dumps({"status":"ok","output":a.output,"total_documente":500,"colectii":{k:len(v) for k,v in data.items()},"ssms":ssms_result},ensure_ascii=False,indent=2))
if __name__=="__main__":
    main()