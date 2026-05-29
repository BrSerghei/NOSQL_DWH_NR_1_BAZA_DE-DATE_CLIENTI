import json
from collections import defaultdict
from pathlib import Path
def citeste_json(cale):
    with open(cale,"r",encoding="utf-8") as f:
        return json.load(f)
def salveaza_json(cale,date):
    cale.parent.mkdir(parents=True,exist_ok=True)
    with open(cale,"w",encoding="utf-8") as f:
        json.dump(date,f,ensure_ascii=False,indent=2)
def gaseste_fisier(folder,nume):
    variante=[folder/f"{nume}.json",folder/f"nr1_clienti.{nume}.json"]
    for v in variante:
        if v.exists():
            return v
    raise FileNotFoundError(f"Nu exista fisierul pentru colectia {nume}")
def incarca_colectii(folder):
    colectii=["categorii","branduri","produse","promotii","magazine","carduri_reducere","clienti","segmente_rfm","comenzi","linii_comanda","companii","contracte_angro","comenzi_angro","linii_comenzi_angro"]
    return {c:citeste_json(gaseste_fisier(folder,c)) for c in colectii}
def index_dupa(lista,camp):
    return {d.get(camp):d for d in lista if d.get(camp) is not None}
def grup_dupa(lista,camp):
    rezultat=defaultdict(list)
    for d in lista:
        rezultat[d.get(camp)].append(d)
    return dict(rezultat)
def fara_campuri(doc,campuri):
    return {k:v for k,v in doc.items() if k not in campuri}
def produs_denormalizat(produs,categorii,branduri,promotii_dupa_produs):
    p=dict(produs)
    p["categorie"]=categorii.get(produs.get("categorie_id"))
    p["brand"]=branduri.get(produs.get("brand_id"))
    p["promotii"]=promotii_dupa_produs.get(produs.get("_id"),[])
    return p
def linie_b2c_denormalizata(linie,produse,categorii,branduri,promotii_dupa_produs):
    l=dict(linie)
    produs=produse.get(linie.get("produs_id"))
    l["produs"]=produs_denormalizat(produs,categorii,branduri,promotii_dupa_produs) if produs else None
    return l
def linie_b2b_denormalizata(linie,produse,categorii,branduri,promotii_dupa_produs):
    l=dict(linie)
    produs=produse.get(linie.get("produs_id"))
    l["produs"]=produs_denormalizat(produs,categorii,branduri,promotii_dupa_produs) if produs else None
    return l
def genereaza_clienti_denormalizat(data):
    categorii=index_dupa(data["categorii"],"_id")
    branduri=index_dupa(data["branduri"],"_id")
    produse=index_dupa(data["produse"],"_id")
    magazine=index_dupa(data["magazine"],"_id")
    carduri=index_dupa(data["carduri_reducere"],"_id")
    promotii_dupa_produs=grup_dupa(data["promotii"],"produs_id")
    segmente_dupa_client=grup_dupa(data["segmente_rfm"],"client_id")
    comenzi_dupa_client=grup_dupa(data["comenzi"],"client_id")
    linii_dupa_comanda=grup_dupa(data["linii_comanda"],"comanda_id")
    rezultat=[]
    for client in data["clienti"]:
        doc=dict(client)
        doc["card_reducere"]=carduri.get(client.get("card_reducere_id"))
        doc["magazin_preferat"]=magazine.get(client.get("magazin_preferat_id"))
        doc["preferinte_categorii_detalii"]=[categorii[c] for c in client.get("preferinte_categorii",[]) if c in categorii]
        segmente=segmente_dupa_client.get(client.get("_id"),[])
        doc["segment_rfm"]=segmente[-1] if segmente else None
        comenzi=[]
        for comanda in comenzi_dupa_client.get(client.get("_id"),[]):
            c=dict(comanda)
            c["magazin"]=magazine.get(comanda.get("magazin_id"))
            c["linii"]= [linie_b2c_denormalizata(l,produse,categorii,branduri,promotii_dupa_produs) for l in linii_dupa_comanda.get(comanda.get("_id"),[])]
            c["numar_linii"]=len(c["linii"])
            c["total_calculat_linii_mdl"]=round(sum(float(l.get("subtotal_mdl",0)) for l in c["linii"]),2)
            comenzi.append(c)
        doc["comenzi"]=comenzi
        doc["numar_comenzi_embedded"]=len(comenzi)
        doc["valoare_totala_comenzi_mdl"]=round(sum(float(c.get("total_net_mdl",0)) for c in comenzi),2)
        rezultat.append(doc)
    return rezultat
def genereaza_companii_denormalizat(data):
    categorii=index_dupa(data["categorii"],"_id")
    branduri=index_dupa(data["branduri"],"_id")
    produse=index_dupa(data["produse"],"_id")
    promotii_dupa_produs=grup_dupa(data["promotii"],"produs_id")
    contracte_dupa_companie=grup_dupa(data["contracte_angro"],"companie_id")
    contracte=index_dupa(data["contracte_angro"],"_id")
    comenzi_dupa_companie=grup_dupa(data["comenzi_angro"],"companie_id")
    linii_dupa_comanda=grup_dupa(data["linii_comenzi_angro"],"comanda_angro_id")
    rezultat=[]
    for companie in data["companii"]:
        doc=dict(companie)
        doc["contracte"]=contracte_dupa_companie.get(companie.get("_id"),[])
        comenzi=[]
        for comanda in comenzi_dupa_companie.get(companie.get("_id"),[]):
            c=dict(comanda)
            c["contract"]=contracte.get(comanda.get("contract_id"))
            c["linii"]=[linie_b2b_denormalizata(l,produse,categorii,branduri,promotii_dupa_produs) for l in linii_dupa_comanda.get(comanda.get("_id"),[])]
            c["numar_linii"]=len(c["linii"])
            c["total_calculat_linii_mdl"]=round(sum(float(l.get("subtotal_mdl",0)) for l in c["linii"]),2)
            comenzi.append(c)
        doc["comenzi_angro"]=comenzi
        doc["numar_comenzi_angro_embedded"]=len(comenzi)
        doc["valoare_totala_comenzi_angro_mdl"]=round(sum(float(c.get("total_net_mdl",0)) for c in comenzi),2)
        rezultat.append(doc)
    return rezultat
def genereaza_produse_denormalizat(data):
    categorii=index_dupa(data["categorii"],"_id")
    branduri=index_dupa(data["branduri"],"_id")
    promotii_dupa_produs=grup_dupa(data["promotii"],"produs_id")
    return [produs_denormalizat(p,categorii,branduri,promotii_dupa_produs) for p in data["produse"]]
def genereaza_denormalizat(data):
    clienti=genereaza_clienti_denormalizat(data)
    companii=genereaza_companii_denormalizat(data)
    produse=genereaza_produse_denormalizat(data)
    return {"clienti_denormalizat":clienti,"companii_denormalizat":companii,"produse_denormalizat":produse,"statistici":{"nr_clienti":len(clienti),"nr_companii":len(companii),"nr_produse":len(produse),"nr_comenzi_b2c":len(data["comenzi"]),"nr_comenzi_b2b":len(data["comenzi_angro"])}}
def main():
    input_folder=Path("JSON_IMPORT_NORMALIZAT")
    output_folder=Path("JSON_DENORMALIZAT")
    if not input_folder.exists():
        input_folder=Path("COLECTII_NORMALIZAT")
    if not input_folder.exists():
        print("Folderul JSON_IMPORT_NORMALIZAT sau COLECTII_NORMALIZAT nu exista.")
        return
    data=incarca_colectii(input_folder)
    denorm=genereaza_denormalizat(data)
    salveaza_json(output_folder/"clienti_denormalizat.json",denorm["clienti_denormalizat"])
    salveaza_json(output_folder/"companii_denormalizat.json",denorm["companii_denormalizat"])
    salveaza_json(output_folder/"produse_denormalizat.json",denorm["produse_denormalizat"])
    salveaza_json(output_folder/"nr1_denormalizat_full.json",denorm)
    print("Conversia din model normalizat in model denormalizat a fost finalizata.")
    print(f"Fisiere salvate in: {output_folder}")
if __name__=="__main__":
    main()