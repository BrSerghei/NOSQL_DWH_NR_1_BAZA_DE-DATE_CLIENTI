import pyodbc
import pandas as pd
conn=pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=DWH_Nr1_Comun;Trusted_Connection=yes;")
exporturi={
"KPI_Clienti_B2C.csv":"SELECT sursa_model,id_client,nume_client,nr_comenzi,valoare_totala_mdl,valoare_medie_comanda_mdl FROM kpi.KPI_Clienti_B2C",
"KPI_Produse_Promotii.csv":"SELECT sursa_model,nr_produse_total,nr_produse_promo,pondere_produse_promo_pct FROM kpi.KPI_Produse_Promotii",
"KPI_Companii_B2B.csv":"SELECT sursa_model,id_companie,denumire_companie,tip_activitate,nr_comenzi_angro,valoare_totala_angro_mdl,valoare_medie_comanda_angro_mdl FROM kpi.KPI_Companii_B2B"
}
for fisier,sql in exporturi.items():
    df=pd.read_sql(sql,conn)
    df.to_csv(fisier,index=False,encoding="utf-8-sig")
    print(f"Export realizat: {fisier} | randuri: {len(df)}")
conn.close()