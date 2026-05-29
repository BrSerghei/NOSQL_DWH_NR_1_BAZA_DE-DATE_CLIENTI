import os
import time
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
MONGO_URI="mongodb://localhost:27017/"
DB_NORMALIZAT="nr1_clienti"
DB_DENORMALIZAT="nr1_denormalizat"
OUTPUT_DIR="CRUD_COMPARATIE_OUTPUT"
os.makedirs(OUTPUT_DIR,exist_ok=True)
run_id=datetime.now().strftime("CRUD%m%d%H%M%S")
client=None
rezultate=[]
def timp(model,operatie,functie):
    start=time.perf_counter()
    rezultat=functie()
    durata=time.perf_counter()-start
    rezultate.append({"Model":model,"Operatie":operatie,"Durata_secunde":round(durata,6),"Observatii":interpreteaza(model,operatie)})
    print(f"{model} | {operatie} | {durata:.6f} secunde")
    return rezultat
def interpreteaza(model,operatie):
    if model=="MongoDB normalizat" and operatie=="READ":
        return "Citire prin referinte si agregare intre colectii"
    if model=="MongoDB denormalizat" and operatie=="READ":
        return "Citire directa din document incorporat"
    if model=="MongoDB normalizat" and operatie in ["UPDATE","DELETE"]:
        return "Modificare precisa pe documente separate"
    if model=="MongoDB denormalizat" and operatie in ["UPDATE","DELETE"]:
        return "Modificare in document complex cu structuri incorporate"
    return "Operatie executata pentru comparatie CRUD"
def conectare():
    global client
    client=MongoClient(MONGO_URI,serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    print("Conexiune MongoDB reusita")
    return client[DB_NORMALIZAT],client[DB_DENORMALIZAT]
def doc_client():
    return {"_id":f"CL_{run_id}","id_client":f"CL_{run_id}","nume":"Test","prenume":"CRUD","telefon":f"+37379{run_id[-6:]}","email":f"client_{run_id.lower()}@nr1.md","adresa":"str. Test CRUD 1, Chisinau","sursa":"test_crud_comparativ"}
def doc_comanda():
    return {"_id":f"CMD_{run_id}","id_comanda":f"CMD_{run_id}","client_id":f"CL_{run_id}","data_comanda":"2026-05-23","status":"test","valoare_bruta_mdl":250.0,"valoare_neta_mdl":235.0,"sursa":"test_crud_comparativ"}
def doc_linie():
    return {"_id":f"LC_{run_id}","id_linie":f"LC_{run_id}","comanda_id":f"CMD_{run_id}","produs_id":"PRD001","cantitate":2,"pret_unitar_mdl":125.0,"valoare_linie_mdl":250.0,"sursa":"test_crud_comparativ"}
def create_normalizat(db):
    def f():
        db.clienti.insert_one(doc_client())
        db.comenzi.insert_one(doc_comanda())
        db.linii_comanda.insert_one(doc_linie())
    return timp("MongoDB normalizat","CREATE",f)
def read_normalizat(db):
    def f():
        pipeline=[{"$match":{"_id":f"CL_{run_id}"}},{"$lookup":{"from":"comenzi","localField":"id_client","foreignField":"client_id","as":"comenzi"}},{"$lookup":{"from":"linii_comanda","localField":"comenzi.id_comanda","foreignField":"comanda_id","as":"linii_comanda"}}]
        return list(db.clienti.aggregate(pipeline))
    return timp("MongoDB normalizat","READ",f)
def update_normalizat(db):
    def f():
        return db.clienti.update_one({"_id":f"CL_{run_id}"},{"$set":{"adresa":"str. Test CRUD 2, Chisinau","email":f"client_actualizat_{run_id.lower()}@nr1.md"}})
    return timp("MongoDB normalizat","UPDATE",f)
def delete_normalizat(db):
    def f():
        db.linii_comanda.delete_many({"comanda_id":f"CMD_{run_id}"})
        db.comenzi.delete_many({"client_id":f"CL_{run_id}"})
        return db.clienti.delete_one({"_id":f"CL_{run_id}"})
    return timp("MongoDB normalizat","DELETE",f)
def doc_client_denorm():
    return {"_id":f"CLD_{run_id}","id_client":f"CLD_{run_id}","nume":"Test","prenume":"CRUD","telefon":f"+37378{run_id[-6:]}","email":f"client_denorm_{run_id.lower()}@nr1.md","adresa":"str. Test CRUD 1, Chisinau","comenzi":[{"id_comanda":f"CMDD_{run_id}","data_comanda":"2026-05-23","status":"test","valoare_bruta_mdl":250.0,"valoare_neta_mdl":235.0,"linii":[{"id_linie":f"LCD_{run_id}","produs_id":"PRD001","cantitate":2,"pret_unitar_mdl":125.0,"valoare_linie_mdl":250.0}]}],"sursa":"test_crud_comparativ"}
def create_denormalizat(db):
    def f():
        return db.clienti_denormalizat.insert_one(doc_client_denorm())
    return timp("MongoDB denormalizat","CREATE",f)
def read_denormalizat(db):
    def f():
        return list(db.clienti_denormalizat.find({"_id":f"CLD_{run_id}"},{"_id":1,"nume":1,"prenume":1,"comenzi":1}))
    return timp("MongoDB denormalizat","READ",f)
def update_denormalizat(db):
    def f():
        return db.clienti_denormalizat.update_one({"_id":f"CLD_{run_id}"},{"$set":{"adresa":"str. Test CRUD 2, Chisinau","comenzi.$[].status":"actualizat"}})
    return timp("MongoDB denormalizat","UPDATE",f)
def delete_denormalizat(db):
    def f():
        db.clienti_denormalizat.update_one({"_id":f"CLD_{run_id}"},{"$pull":{"comenzi":{"id_comanda":f"CMDD_{run_id}"}}})
        return db.clienti_denormalizat.delete_one({"_id":f"CLD_{run_id}"})
    return timp("MongoDB denormalizat","DELETE",f)
def export_rezultate():
    df=pd.DataFrame(rezultate)
    tabel=df.pivot_table(index="Operatie",columns="Model",values="Durata_secunde",aggfunc="mean").reset_index()
    tabel["Diferenta_secunde"]=(tabel.get("MongoDB normalizat",0)-tabel.get("MongoDB denormalizat",0)).round(6)
    observatii=pd.DataFrame({"Operatie":["CREATE","READ","UPDATE","DELETE"],"Interpretare":["Inserarea denormalizata include datele intr-un singur document, iar modelul normalizat distribuie datele in colectii separate.","Citirea denormalizata este orientata spre acces direct, iar citirea normalizata cere agregare intre colectii.","Actualizarea normalizata este mai precisa, iar actualizarea denormalizata poate atinge structuri incorporate.","Stergerea normalizata elimina documente separate, iar stergerea denormalizata necesita modificarea documentului complex."]})
    excel=os.path.join(OUTPUT_DIR,"CRUD_Comparatie_Nr1_Normalizat_Denormalizat.xlsx")
    csv=os.path.join(OUTPUT_DIR,"CRUD_Comparatie_Nr1_Normalizat_Denormalizat.csv")
    df.to_csv(csv,index=False,encoding="utf-8-sig")
    with pd.ExcelWriter(excel,engine="openpyxl") as writer:
        df.to_excel(writer,sheet_name="Rezultate_detaliate",index=False)
        tabel.to_excel(writer,sheet_name="Sinteza",index=False)
        observatii.to_excel(writer,sheet_name="Interpretare",index=False)
    ax=tabel.set_index("Operatie")[[c for c in ["MongoDB normalizat","MongoDB denormalizat"] if c in tabel.columns]].plot(kind="bar",figsize=(9,5))
    ax.set_title("Comparatie CRUD Nr.1: normalizat vs denormalizat")
    ax.set_xlabel("Operatie CRUD")
    ax.set_ylabel("Durata, secunde")
    plt.tight_layout()
    grafic=os.path.join(OUTPUT_DIR,"CRUD_Comparatie_Nr1_Normalizat_Denormalizat.png")
    plt.savefig(grafic,dpi=200)
    print("Export Excel:",excel)
    print("Export CSV:",csv)
    print("Export grafic:",grafic)
def main():
    try:
        db_norm,db_denorm=conectare()
        print("Run ID:",run_id)
        create_normalizat(db_norm)
        read_normalizat(db_norm)
        update_normalizat(db_norm)
        delete_normalizat(db_norm)
        create_denormalizat(db_denorm)
        read_denormalizat(db_denorm)
        update_denormalizat(db_denorm)
        delete_denormalizat(db_denorm)
        export_rezultate()
    except ConnectionFailure:
        print("Conexiune MongoDB esuata")
    finally:
        if client:
            client.close()
if __name__=="__main__":
    main()
