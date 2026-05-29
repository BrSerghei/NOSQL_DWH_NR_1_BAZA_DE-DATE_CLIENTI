import json
import time
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure,DuplicateKeyError
from prettytable import PrettyTable
MONGO_URI="mongodb://localhost:27017/"
DB_NORMALIZAT="nr1_clienti"
DB_DENORMALIZAT="nr1_denormalizat"
def conectare():
    try:
        client=MongoClient(MONGO_URI,serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print("Conectare reușită la MongoDB.")
        return client
    except ConnectionFailure:
        print("Conectare eșuată la MongoDB.")
        exit()
def cod(prefix):
    return f"{prefix}_{datetime.now().strftime('%Y%m%d%H%M%S%f')[:17]}"
def nr_float(text):
    try:
        return float(text.replace(",","."))
    except:
        return 0.0
def nr_int(text):
    try:
        return int(text)
    except:
        return 0
def colectii_pentru_model(db_name):
    if db_name==DB_NORMALIZAT:
        return ["clienti","comenzi","linii_comanda","companii"]
    return ["clienti_denormalizat","companii_denormalizat","produse_denormalizat"]
def titlu_document(col,d):
    if col in ["clienti","clienti_denormalizat"]:
        return f"{d.get('id_client','')} | {d.get('nume','')} {d.get('prenume','')} | {d.get('telefon','')}"
    if col=="comenzi":
        return f"{d.get('id_comanda','')} | client: {d.get('client_id','')} | {d.get('valoare_totala_mdl',0)} MDL"
    if col=="linii_comanda":
        return f"{d.get('id_linie','')} | comanda: {d.get('comanda_id','')} | produs: {d.get('produs_id','')}"
    if col in ["companii","companii_denormalizat"]:
        return f"{d.get('id_companie','')} | {d.get('denumire','')} | {d.get('tip_activitate','')}"
    if col=="produse_denormalizat":
        return f"{d.get('id_produs','')} | {d.get('denumire','')} | {d.get('pret_mdl',0)} MDL"
    return str(d.get("_id",""))
def afisare_documente(docs):
    docs=list(docs)
    if not docs:
        print("Nu au fost găsite documente.")
        return
    keys=[]
    for d in docs:
        for k in d.keys():
            if k not in keys:
                keys.append(k)
    keys=keys[:8]
    table=PrettyTable(keys)
    for d in docs:
        row=[]
        for k in keys:
            v=d.get(k,"")
            if isinstance(v,(dict,list)):
                v=json.dumps(v,ensure_ascii=False)[:80]
            row.append(str(v)[:80])
        table.add_row(row)
    print(table)
    print(f"Total documente afișate: {len(docs)}")
def alege_document(db,col):
    docs=list(db[col].find().sort("_id",-1).limit(20))
    if not docs:
        print("Nu există documente în colecția aleasă.")
        return None
    print("\nAlege documentul:")
    for i,d in enumerate(docs,1):
        print(f"{i}. {titlu_document(col,d)}")
    opt=nr_int(input("Număr document: ").strip())
    if opt<1 or opt>len(docs):
        print("Opțiune invalidă.")
        return None
    return docs[opt-1]
def validare_generala(doc):
    if not isinstance(doc,dict):
        return False,"Documentul trebuie să fie obiect JSON."
    if "_id" not in doc:
        return False,"Lipsește câmpul _id."
    return True,"Structură JSON validă."
def validare_client_normalizat(doc):
    necesare=["_id","id_client","nume","prenume","email","telefon"]
    for k in necesare:
        if k not in doc or doc[k] in ["",None]:
            return False,f"Lipsește câmpul obligatoriu: {k}"
    if "@" not in doc["email"]:
        return False,"Email invalid."
    return True,"Client normalizat valid."
def validare_comanda_normalizat(doc):
    necesare=["_id","id_comanda","client_id","data_comanda","valoare_totala_mdl","status"]
    for k in necesare:
        if k not in doc or doc[k] in ["",None]:
            return False,f"Lipsește câmpul obligatoriu: {k}"
    if doc["valoare_totala_mdl"]<0:
        return False,"Valoarea comenzii nu poate fi negativă."
    return True,"Comandă normalizată validă."
def validare_linie_normalizat(doc):
    necesare=["_id","id_linie","comanda_id","produs_id","cantitate","pret_unitar_mdl"]
    for k in necesare:
        if k not in doc or doc[k] in ["",None]:
            return False,f"Lipsește câmpul obligatoriu: {k}"
    if doc["cantitate"]<=0 or doc["pret_unitar_mdl"]<0:
        return False,"Cantitatea sau prețul sunt incorecte."
    return True,"Linie de comandă validă."
def validare_companie_normalizat(doc):
    necesare=["_id","id_companie","denumire","idno","tip_activitate","telefon","email"]
    for k in necesare:
        if k not in doc or doc[k] in ["",None]:
            return False,f"Lipsește câmpul obligatoriu: {k}"
    return True,"Companie normalizată validă."
def validare_client_denormalizat(doc):
    necesare=["_id","id_client","nume","prenume","email","telefon","comenzi"]
    for k in necesare:
        if k not in doc:
            return False,f"Lipsește câmpul obligatoriu: {k}"
    if not isinstance(doc["comenzi"],list):
        return False,"Câmpul comenzi trebuie să fie listă."
    return True,"Client denormalizat valid."
def validare_companie_denormalizat(doc):
    necesare=["_id","id_companie","denumire","idno","tip_activitate","comenzi_angro"]
    for k in necesare:
        if k not in doc:
            return False,f"Lipsește câmpul obligatoriu: {k}"
    if not isinstance(doc["comenzi_angro"],list):
        return False,"Câmpul comenzi_angro trebuie să fie listă."
    return True,"Companie denormalizată validă."
def validare_produs_denormalizat(doc):
    necesare=["_id","id_produs","denumire","categorie","pret_mdl","promotii"]
    for k in necesare:
        if k not in doc:
            return False,f"Lipsește câmpul obligatoriu: {k}"
    if not isinstance(doc["promotii"],list):
        return False,"Câmpul promotii trebuie să fie listă."
    if doc["pret_mdl"]<0:
        return False,"Preț invalid."
    return True,"Produs denormalizat valid."
def validare_dupa_colectie(db_name,col,doc):
    ok,msg=validare_generala(doc)
    if not ok:
        return ok,msg
    if db_name==DB_NORMALIZAT and col=="clienti":
        return validare_client_normalizat(doc)
    if db_name==DB_NORMALIZAT and col=="comenzi":
        return validare_comanda_normalizat(doc)
    if db_name==DB_NORMALIZAT and col=="linii_comanda":
        return validare_linie_normalizat(doc)
    if db_name==DB_NORMALIZAT and col=="companii":
        return validare_companie_normalizat(doc)
    if db_name==DB_DENORMALIZAT and col=="clienti_denormalizat":
        return validare_client_denormalizat(doc)
    if db_name==DB_DENORMALIZAT and col=="companii_denormalizat":
        return validare_companie_denormalizat(doc)
    if db_name==DB_DENORMALIZAT and col=="produse_denormalizat":
        return validare_produs_denormalizat(doc)
    return True,"Validare generală reușită."
def verificare_consistenta(db_name,db,col,doc):
    if db_name==DB_NORMALIZAT and col=="comenzi":
        if not db["clienti"].find_one({"id_client":doc["client_id"]}):
            return False,"Clientul indicat nu există în colecția clienti."
    if db_name==DB_NORMALIZAT and col=="linii_comanda":
        if not db["comenzi"].find_one({"id_comanda":doc["comanda_id"]}):
            return False,"Comanda indicată nu există în colecția comenzi."
        if not db["produse"].find_one({"id_produs":doc["produs_id"]}):
            return False,"Produsul indicat nu există în colecția produse."
    return True,"Consistență verificată."
def creare_client_normalizat():
    idc=cod("CLI")
    return {"_id":idc,"id_client":idc,"nume":input("Nume client: ").strip(),"prenume":input("Prenume client: ").strip(),"email":input("Email: ").strip(),"telefon":input("Telefon: ").strip(),"oras":input("Oraș: ").strip(),"data_inregistrare":datetime.now().strftime("%Y-%m-%d"),"status":"activ"}
def creare_comanda_normalizat(db):
    idc=cod("CMD")
    afisare_documente(db["clienti"].find({},{"_id":0,"id_client":1,"nume":1,"prenume":1}).limit(10))
    return {"_id":idc,"id_comanda":idc,"client_id":input("ID client existent: ").strip(),"data_comanda":datetime.now().strftime("%Y-%m-%d"),"valoare_totala_mdl":nr_float(input("Valoare totală MDL: ").strip()),"status":"finalizată","tip_flux":"B2C"}
def creare_linie_normalizat(db):
    idl=cod("LIN")
    afisare_documente(db["comenzi"].find({},{"_id":0,"id_comanda":1,"client_id":1,"valoare_totala_mdl":1}).limit(10))
    comanda_id=input("ID comandă existentă: ").strip()
    afisare_documente(db["produse"].find({},{"_id":0,"id_produs":1,"denumire":1,"pret_mdl":1}).limit(10))
    produs_id=input("ID produs existent: ").strip()
    cantitate=nr_int(input("Cantitate: ").strip())
    pret=nr_float(input("Preț unitar MDL: ").strip())
    return {"_id":idl,"id_linie":idl,"comanda_id":comanda_id,"produs_id":produs_id,"cantitate":cantitate,"pret_unitar_mdl":pret,"valoare_linie_mdl":round(cantitate*pret,2)}
def creare_companie_normalizat():
    idc=cod("CMP")
    return {"_id":idc,"id_companie":idc,"denumire":input("Denumire companie: ").strip(),"idno":input("IDNO: ").strip(),"tip_activitate":input("Tip activitate: ").strip(),"telefon":input("Telefon: ").strip(),"email":input("Email: ").strip(),"adresa_livrare":input("Adresă livrare: ").strip(),"tva_inregistrat":True}
def creare_client_denormalizat():
    idc=cod("CLD")
    nume=input("Nume client: ").strip()
    prenume=input("Prenume client: ").strip()
    email=input("Email: ").strip()
    telefon=input("Telefon: ").strip()
    oras=input("Oraș: ").strip()
    comenzi=[]
    if input("Adăugăm o comandă inițială? da/nu: ").strip().lower()=="da":
        comenzi.append({"id_comanda":cod("DCMD"),"data_comanda":datetime.now().strftime("%Y-%m-%d"),"valoare_totala_mdl":nr_float(input("Valoare comandă MDL: ").strip()),"status":"finalizată","linii":[]})
    total=sum(c.get("valoare_totala_mdl",0) for c in comenzi)
    nr=len(comenzi)
    medie=round(total/nr,2) if nr>0 else 0
    return {"_id":idc,"id_client":idc,"nume":nume,"prenume":prenume,"email":email,"telefon":telefon,"oras":oras,"nr_comenzi":nr,"valoare_totala_mdl":total,"valoare_medie_comanda_mdl":medie,"comenzi":comenzi}
def creare_companie_denormalizat():
    idc=cod("CPD")
    denumire=input("Denumire companie: ").strip()
    idno=input("IDNO: ").strip()
    tip=input("Tip activitate: ").strip()
    comenzi=[]
    if input("Adăugăm o comandă angro inițială? da/nu: ").strip().lower()=="da":
        comenzi.append({"id_comanda_angro":cod("ACMD"),"data_comanda":datetime.now().strftime("%Y-%m-%d"),"valoare_totala_angro_mdl":nr_float(input("Valoare comandă angro MDL: ").strip()),"status":"finalizată","linii":[]})
    total=sum(c.get("valoare_totala_angro_mdl",0) for c in comenzi)
    nr=len(comenzi)
    medie=round(total/nr,2) if nr>0 else 0
    return {"_id":idc,"id_companie":idc,"denumire":denumire,"idno":idno,"tip_activitate":tip,"nr_comenzi_angro":nr,"valoare_totala_angro_mdl":total,"valoare_medie_comanda_angro_mdl":medie,"comenzi_angro":comenzi}
def creare_produs_denormalizat():
    idp=cod("PRD")
    promotii=[]
    if input("Produsul are promoție? da/nu: ").strip().lower()=="da":
        promotii.append({"id_promotie":cod("PRO"),"denumire":"Promoție introdusă manual","discount_pct":nr_float(input("Discount %: ").strip()),"data_start":datetime.now().strftime("%Y-%m-%d")})
    return {"_id":idp,"id_produs":idp,"denumire":input("Denumire produs: ").strip(),"categorie":input("Categorie: ").strip(),"brand":input("Brand: ").strip(),"pret_mdl":nr_float(input("Preț MDL: ").strip()),"promotii":promotii,"nr_promotii":len(promotii)}
def creare_asistata(db_name,db,col):
    if db_name==DB_NORMALIZAT and col=="clienti":
        return creare_client_normalizat()
    if db_name==DB_NORMALIZAT and col=="comenzi":
        return creare_comanda_normalizat(db)
    if db_name==DB_NORMALIZAT and col=="linii_comanda":
        return creare_linie_normalizat(db)
    if db_name==DB_NORMALIZAT and col=="companii":
        return creare_companie_normalizat()
    if db_name==DB_DENORMALIZAT and col=="clienti_denormalizat":
        return creare_client_denormalizat()
    if db_name==DB_DENORMALIZAT and col=="companii_denormalizat":
        return creare_companie_denormalizat()
    if db_name==DB_DENORMALIZAT and col=="produse_denormalizat":
        return creare_produs_denormalizat()
    return None
def operatie_create(db_name,db,col):
    print("\nInserare asistată.")
    doc=creare_asistata(db_name,db,col)
    if not doc:
        return
    ok,msg=validare_dupa_colectie(db_name,col,doc)
    if not ok:
        print("Validare eșuată:",msg)
        return
    ok,msg=verificare_consistenta(db_name,db,col,doc)
    if not ok:
        print("Consistență eșuată:",msg)
        return
    start=time.perf_counter()
    try:
        db[col].insert_one(doc)
        end=time.perf_counter()
        print(f"Document inserat cu succes. Durata: {end-start:.6f} secunde.")
    except DuplicateKeyError:
        print("Document duplicat. Inserarea a fost anulată.")
def operatie_read(db,col):
    print("\nCitire documente:")
    print("1. Ultimele 10 documente")
    print("2. Caută după text")
    opt=input("Opțiune: ").strip()
    start=time.perf_counter()
    if opt=="1":
        afisare_documente(db[col].find().sort("_id",-1).limit(10))
    elif opt=="2":
        txt=input("Text căutat: ").strip()
        filtre=[]
        for camp in ["nume","prenume","denumire","telefon","email","id_client","id_companie","id_produs","id_comanda"]:
            filtre.append({camp:{"$regex":txt,"$options":"i"}})
        afisare_documente(db[col].find({"$or":filtre}).limit(20))
    else:
        print("Opțiune invalidă.")
    end=time.perf_counter()
    print(f"Durata citirii: {end-start:.6f} secunde.")
def campuri_editabile(col):
    if col in ["clienti","clienti_denormalizat"]:
        return ["nume","prenume","email","telefon","oras","status"]
    if col=="comenzi":
        return ["valoare_totala_mdl","status"]
    if col=="linii_comanda":
        return ["cantitate","pret_unitar_mdl"]
    if col in ["companii","companii_denormalizat"]:
        return ["denumire","tip_activitate","telefon","email","adresa_livrare"]
    if col=="produse_denormalizat":
        return ["denumire","categorie","brand","pret_mdl"]
    return []
def conversie_valoare(v):
    if v.lower() in ["true","false"]:
        return v.lower()=="true"
    if v.replace(".","",1).replace("-","",1).isdigit():
        return float(v) if "." in v else int(v)
    return v
def operatie_update(db_name,db,col):
    print("\nActualizare asistată.")
    doc=alege_document(db,col)
    if not doc:
        return
    campuri=campuri_editabile(col)
    if not campuri:
        print("Nu există câmpuri editabile definite pentru colecția dată.")
        return
    print("\nAlege ce dorești să modifici:")
    for i,c in enumerate(campuri,1):
        print(f"{i}. {c} | valoare curentă: {doc.get(c,'')}")
    opt=nr_int(input("Opțiune: ").strip())
    if opt<1 or opt>len(campuri):
        print("Opțiune invalidă.")
        return
    camp=campuri[opt-1]
    valoare=conversie_valoare(input("Valoare nouă: ").strip())
    doc_nou=dict(doc)
    doc_nou[camp]=valoare
    if col=="linii_comanda" and camp in ["cantitate","pret_unitar_mdl"]:
        doc_nou["valoare_linie_mdl"]=round(float(doc_nou.get("cantitate",0))*float(doc_nou.get("pret_unitar_mdl",0)),2)
    ok,msg=validare_dupa_colectie(db_name,col,doc_nou)
    if not ok:
        print("Actualizarea nu este permisă:",msg)
        return
    confirm=input(f"Confirmați modificarea câmpului {camp}? da/nu: ").strip().lower()
    if confirm!="da":
        print("Actualizare anulată.")
        return
    start=time.perf_counter()
    setari={camp:valoare}
    if col=="linii_comanda" and "valoare_linie_mdl" in doc_nou:
        setari["valoare_linie_mdl"]=doc_nou["valoare_linie_mdl"]
    rez=db[col].update_one({"_id":doc["_id"]},{"$set":setari})
    end=time.perf_counter()
    print(f"Documente modificate: {rez.modified_count}. Durata: {end-start:.6f} secunde.")
def operatie_delete(db,col):
    print("\nȘtergere asistată.")
    doc=alege_document(db,col)
    if not doc:
        return
    print("\nDocument selectat:")
    print(titlu_document(col,doc))
    print("1. Șterge documentul")
    print("2. Anulează")
    opt=input("Opțiune: ").strip()
    if opt!="1":
        print("Ștergere anulată.")
        return
    confirm=input("Confirmați definitiv ștergerea? da/nu: ").strip().lower()
    if confirm!="da":
        print("Ștergere anulată.")
        return
    start=time.perf_counter()
    rez=db[col].delete_one({"_id":doc["_id"]})
    end=time.perf_counter()
    print(f"Documente șterse: {rez.deleted_count}. Durata: {end-start:.6f} secunde.")
def test_validare(db_name,db,col):
    print("\nValidare asistată.")
    print("1. Validează un document ales")
    print("2. Validează ultimele 20 documente")
    opt=input("Opțiune: ").strip()
    docs=[]
    if opt=="1":
        doc=alege_document(db,col)
        if doc:
            docs=[doc]
    elif opt=="2":
        docs=list(db[col].find().sort("_id",-1).limit(20))
    else:
        print("Opțiune invalidă.")
        return
    if not docs:
        print("Nu există documente pentru validare.")
        return
    table=PrettyTable(["Document","Structură","Consistență"])
    for d in docs:
        ok1,msg1=validare_dupa_colectie(db_name,col,d)
        ok2,msg2=verificare_consistenta(db_name,db,col,d)
        table.add_row([titlu_document(col,d)[:45],"OK" if ok1 else msg1,"OK" if ok2 else msg2])
    print(table)
def statistici(db_name,db):
    table=PrettyTable(["Colecție","Documente"])
    for c in colectii_pentru_model(db_name):
        table.add_row([c,db[c].count_documents({})])
    print(table)
def alege_model(client):
    print("\nAlege modelul bazei de date:")
    print("1. MongoDB normalizat: nr1_clienti")
    print("2. MongoDB denormalizat: nr1_denormalizat")
    opt=input("Opțiune: ").strip()
    if opt=="1":
        return DB_NORMALIZAT,client[DB_NORMALIZAT]
    if opt=="2":
        return DB_DENORMALIZAT,client[DB_DENORMALIZAT]
    print("Opțiune invalidă.")
    return alege_model(client)
def alege_colectie(db_name):
    colectii=colectii_pentru_model(db_name)
    print("\nAlege colecția:")
    for i,c in enumerate(colectii,1):
        print(f"{i}. {c}")
    opt=nr_int(input("Opțiune: ").strip())
    if opt>=1 and opt<=len(colectii):
        return colectii[opt-1]
    print("Opțiune invalidă.")
    return alege_colectie(db_name)
def meniu_crud(db_name,db,col):
    while True:
        print(f"\nColecție activă: {db_name}.{col}")
        print("1. CREATE - adaugă document")
        print("2. READ - afișează/caută documente")
        print("3. UPDATE - modifică document prin alegere din listă")
        print("4. DELETE - șterge document prin alegere din listă")
        print("5. VALIDARE - verificare automată")
        print("6. Înapoi")
        opt=input("Opțiune: ").strip()
        if opt=="1":
            operatie_create(db_name,db,col)
        elif opt=="2":
            operatie_read(db,col)
        elif opt=="3":
            operatie_update(db_name,db,col)
        elif opt=="4":
            operatie_delete(db,col)
        elif opt=="5":
            test_validare(db_name,db,col)
        elif opt=="6":
            break
        else:
            print("Opțiune invalidă.")
def meniu_model(client):
    db_name,db=alege_model(client)
    while True:
        print(f"\nModel activ: {db_name}")
        print("1. Alege colecția și execută CRUD")
        print("2. Statistici documente")
        print("3. Schimbă modelul")
        print("4. Ieșire")
        opt=input("Opțiune: ").strip()
        if opt=="1":
            col=alege_colectie(db_name)
            meniu_crud(db_name,db,col)
        elif opt=="2":
            statistici(db_name,db)
        elif opt=="3":
            db_name,db=alege_model(client)
        elif opt=="4":
            break
        else:
            print("Opțiune invalidă.")
def main():
    client=conectare()
    print("Aplicație CRUD MongoDB pentru proiectul Lab4_BD - Rețeaua de magazine Nr.1")
    print("Operațiile sunt asistate pentru utilizatori fără cunoștințe de sintaxă MongoDB.")
    meniu_model(client)
    client.close()
    print("Conexiunea a fost închisă.")
if __name__=="__main__":
    main()