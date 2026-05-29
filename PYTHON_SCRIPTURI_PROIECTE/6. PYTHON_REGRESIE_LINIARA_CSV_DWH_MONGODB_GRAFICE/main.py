import os
import math
import pandas as pd
from pymongo import MongoClient
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score,mean_absolute_error,mean_squared_error
from sqlalchemy import create_engine
from urllib.parse import quote_plus
OUTPUT_DIR="REGRESIE_OUTPUT"
SQL_CONN="DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=DWH_Nr1_Comun;Trusted_Connection=yes;"
MONGO_URI="mongodb://localhost:27017/"
MONGO_DB="nr1_denormalizat"
os.makedirs(OUTPUT_DIR,exist_ok=True)
def engine_sql():
    return create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(SQL_CONN)}")
def metrici(y,p):
    return r2_score(y,p),mean_absolute_error(y,p),math.sqrt(mean_squared_error(y,p))
def regresie(df,kpi,model_sursa,y_col,x_cols):
    df=df[x_cols+[y_col]].dropna()
    X=df[x_cols]
    y=df[y_col]
    model=LinearRegression()
    model.fit(X,y)
    pred=model.predict(X)
    r2,mae,rmse=metrici(y,pred)
    coef=pd.DataFrame({"KPI":kpi,"Model":model_sursa,"Variabila":x_cols,"Coeficient":model.coef_})
    coef.loc[len(coef)]=[kpi,model_sursa,"Intercept",model.intercept_]
    rez=df.copy()
    rez["KPI"]=kpi
    rez["Model"]=model_sursa
    rez["Y_REAL"]=y
    rez["Y_PREZIS"]=pred
    rez["EROARE"]=rez["Y_REAL"]-rez["Y_PREZIS"]
    met=pd.DataFrame([{"KPI":kpi,"Model":model_sursa,"R2":r2,"MAE":mae,"RMSE":rmse}])
    return coef,met,rez
def date_sql():
    eng=engine_sql()
    q1="SELECT CAST(nr_comenzi AS FLOAT) AS X1_NR_COMENZI,CAST(valoare_totala_mdl AS FLOAT) AS X2_VALOARE_TOTALA_MDL,CAST(valoare_medie_comanda_mdl AS FLOAT) AS Y_VALOARE_MEDIE_COMANDA_MDL FROM kpi.KPI_Clienti_B2C WHERE nr_comenzi IS NOT NULL AND valoare_totala_mdl IS NOT NULL AND valoare_medie_comanda_mdl IS NOT NULL"
    q2="SELECT CAST(nr_comenzi_angro AS FLOAT) AS X1_NR_COMENZI_ANGRO,CAST(valoare_totala_angro_mdl AS FLOAT) AS X2_VALOARE_TOTALA_ANGRO_MDL,CAST(valoare_medie_comanda_angro_mdl AS FLOAT) AS Y_VALOARE_MEDIE_COMANDA_ANGRO_MDL FROM kpi.KPI_Companii_B2B WHERE nr_comenzi_angro IS NOT NULL AND valoare_totala_angro_mdl IS NOT NULL AND valoare_medie_comanda_angro_mdl IS NOT NULL"
    return pd.read_sql(q1,eng),pd.read_sql(q2,eng)
def numar(doc,chei):
    for k in chei:
        if k in doc and doc[k] not in [None,""]:
            return float(doc[k])
    return 0.0
def valoare_comanda(c):
    for k in ["valoare_neta_mdl","valoare_totala_mdl","total_mdl","suma_mdl","valoare"]:
        if k in c and c[k] not in [None,""]:
            return float(c[k])
    return 0.0
def date_mongo():
    client=MongoClient(MONGO_URI)
    db=client[MONGO_DB]
    clienti=[]
    for d in db["clienti_denormalizat"].find({}):
        nr=numar(d,["nr_comenzi","numar_comenzi"])
        total=numar(d,["valoare_totala_mdl","valoare_totala_comenzi_mdl","total_comenzi_mdl"])
        if nr==0 and isinstance(d.get("comenzi"),list):
            nr=len(d["comenzi"])
        if total==0 and isinstance(d.get("comenzi"),list):
            total=sum(valoare_comanda(c) for c in d["comenzi"])
        y=numar(d,["valoare_medie_comanda_mdl","valoare_medie_mdl"])
        if y==0 and nr>0:
            y=total/nr
        if nr>0 and total>0 and y>0:
            clienti.append({"X1_NR_COMENZI":nr,"X2_VALOARE_TOTALA_MDL":total,"Y_VALOARE_MEDIE_COMANDA_MDL":y})
    companii=[]
    for d in db["companii_denormalizat"].find({}):
        nr=numar(d,["nr_comenzi_angro","numar_comenzi_angro"])
        total=numar(d,["valoare_totala_angro_mdl","valoare_totala_comenzi_angro_mdl","total_angro_mdl"])
        if nr==0 and isinstance(d.get("comenzi_angro"),list):
            nr=len(d["comenzi_angro"])
        if total==0 and isinstance(d.get("comenzi_angro"),list):
            total=sum(valoare_comanda(c) for c in d["comenzi_angro"])
        y=numar(d,["valoare_medie_comanda_angro_mdl","valoare_medie_angro_mdl"])
        if y==0 and nr>0:
            y=total/nr
        if nr>0 and total>0 and y>0:
            companii.append({"X1_NR_COMENZI_ANGRO":nr,"X2_VALOARE_TOTALA_ANGRO_MDL":total,"Y_VALOARE_MEDIE_COMANDA_ANGRO_MDL":y})
    client.close()
    return pd.DataFrame(clienti),pd.DataFrame(companii)
def main():
    df_sql_b2c,df_sql_b2b=date_sql()
    df_mongo_b2c,df_mongo_b2b=date_mongo()
    coef=[]
    met=[]
    pred=[]
    c,m,p=regresie(df_sql_b2c,"KPI_CLIENTI_B2C","DWH_SQL_SERVER","Y_VALOARE_MEDIE_COMANDA_MDL",["X1_NR_COMENZI","X2_VALOARE_TOTALA_MDL"])
    coef.append(c);met.append(m);pred.append(p)
    c,m,p=regresie(df_mongo_b2c,"KPI_CLIENTI_B2C","MONGODB_DENORMALIZAT","Y_VALOARE_MEDIE_COMANDA_MDL",["X1_NR_COMENZI","X2_VALOARE_TOTALA_MDL"])
    coef.append(c);met.append(m);pred.append(p)
    c,m,p=regresie(df_sql_b2b,"KPI_COMPANII_B2B","DWH_SQL_SERVER","Y_VALOARE_MEDIE_COMANDA_ANGRO_MDL",["X1_NR_COMENZI_ANGRO","X2_VALOARE_TOTALA_ANGRO_MDL"])
    coef.append(c);met.append(m);pred.append(p)
    c,m,p=regresie(df_mongo_b2b,"KPI_COMPANII_B2B","MONGODB_DENORMALIZAT","Y_VALOARE_MEDIE_COMANDA_ANGRO_MDL",["X1_NR_COMENZI_ANGRO","X2_VALOARE_TOTALA_ANGRO_MDL"])
    coef.append(c);met.append(m);pred.append(p)
    df_coef=pd.concat(coef,ignore_index=True)
    df_met=pd.concat(met,ignore_index=True)
    df_pred=pd.concat(pred,ignore_index=True)
    df_met.to_csv(os.path.join(OUTPUT_DIR,"comparatie_metrici_regresie.csv"),index=False,encoding="utf-8-sig")
    with pd.ExcelWriter(os.path.join(OUTPUT_DIR,"Regresie_Comparatie_SQLServer_MongoDB.xlsx"),engine="openpyxl") as writer:
        df_met.to_excel(writer,sheet_name="Comparatie_metrici",index=False)
        df_coef.to_excel(writer,sheet_name="Coeficienti",index=False)
        df_pred.to_excel(writer,sheet_name="Predictii",index=False)
    print("Export finalizat:",OUTPUT_DIR)
    print(df_met)
if __name__=="__main__":
    main()