import os,json,time,math,random
from datetime import datetime,timedelta
import pandas as pd
import numpy as np
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError,BulkWriteError
from bson import ObjectId
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score,mean_absolute_error,mean_squared_error
import plotly.express as px
import plotly.graph_objects as go
try:
 from streamlit_echarts import st_echarts
except Exception:
 st_echarts=None
try:
 from sqlalchemy import create_engine
 from urllib.parse import quote_plus
except Exception:
 create_engine=None
 quote_plus=None
st.set_page_config(page_title="NR.1 Clienți",layout="wide",initial_sidebar_state="expanded")
MONGO_URI=os.getenv("MONGO_URI","mongodb://localhost:27017/")
DBN=os.getenv("DB_NORMALIZAT","nr1_clienti")
DBD=os.getenv("DB_DENORMALIZAT","nr1_denormalizat")
SQL_CONN=os.getenv("SQL_CONN","DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=DWH_Nr1_Comun;Trusted_Connection=yes;")
N_COLS=["categorii","branduri","produse","promotii","magazine","carduri_reducere","clienti","segmente_rfm","comenzi","linii_comanda","companii","contracte_angro","comenzi_angro","linii_comenzi_angro"]
D_COLS=["clienti_denormalizat","companii_denormalizat","produse_denormalizat","nr1_denormalizat_full"]
st.markdown("""
<style>
[data-testid="stSidebar"]{background:#0b0b0b;color:white}
[data-testid="stSidebar"] *{color:white!important}
.main-title{font-size:32px;font-weight:850;color:#111;margin-bottom:10px}
.card{background:white;border:1px solid #eee;border-radius:18px;padding:16px;box-shadow:0 6px 20px rgba(0,0,0,.05)}
.decision{background:#eef8f1;border-left:5px solid #1fa339;border-radius:12px;padding:14px;margin:12px 0;color:#123}
.warn{background:#fff4e8;border-left:5px solid #ff7f11;border-radius:12px;padding:14px;margin:12px 0;color:#222}
</style>
""",unsafe_allow_html=True)
@st.cache_resource
def client():
 c=MongoClient(MONGO_URI,serverSelectionTimeoutMS=5000)
 c.admin.command("ping")
 return c
def dbase(model):
 return client()[DBN if model=="normalizat" else DBD]
def cols(model):
 return N_COLS if model=="normalizat" else D_COLS
def oid(v):
 return str(v) if isinstance(v,ObjectId) else v
def js(v):
 try:
  return json.dumps(v,ensure_ascii=False,default=str)
 except Exception:
  return str(v)
def rowdoc(x):
 r={}
 for k,v in x.items():
  if isinstance(v,ObjectId): r[k]=str(v)
  elif isinstance(v,(list,dict)): r[k]=js(v)[:900]
  else: r[k]=v
 return r
def now(prefix):
 return f"{prefix}_{datetime.now().strftime('%Y%m%d%H%M%S%f')[:18]}"
def fnum(v):
 try:
  return float(str(v).replace(",","."))
 except Exception:
  return 0.0
def title(col,x):
 if col in ["clienti","clienti_denormalizat"]: return f"{x.get('id_client',x.get('_id',''))} | {x.get('nume','')} {x.get('prenume','')} | {x.get('telefon','')}"
 if col in ["companii","companii_denormalizat"]: return f"{x.get('id_companie',x.get('_id',''))} | {x.get('denumire','')} | {x.get('tip_activitate','')}"
 if col in ["produse","produse_denormalizat"]: return f"{x.get('id_produs',x.get('_id',''))} | {x.get('denumire','')} | {x.get('pret_mdl',0)} MDL"
 if col in ["comenzi","comenzi_angro"]: return f"{x.get('id_comanda',x.get('id_comanda_angro',x.get('_id','')))} | {x.get('valoare_totala_mdl',x.get('valoare_totala_angro_mdl',x.get('total_net_mdl',0)))} MDL"
 return str(x.get("_id","document"))
def header(t):
 st.markdown(f"<div class='main-title'>{t}</div>",unsafe_allow_html=True)
def all_sources():
 return [("normalizat",c) for c in N_COLS]+[("denormalizat",c) for c in D_COLS]
def search_docs(q,sources,limit=200):
 out=[]
 q=(q or "").lower()
 for m,c in sources:
  try:
   for x in dbase(m)[c].find().sort("_id",-1).limit(2500):
    s=js(x).lower()
    if not q or q in s:
     r=rowdoc(x)
     r["model"]=m
     r["colectie"]=c
     r["titlu"]=title(c,x)
     out.append(r)
     if len(out)>=limit: return pd.DataFrame(out)
  except Exception:
   pass
 return pd.DataFrame(out)
def kpi_normalizat():
 db=dbase("normalizat")
 rows=[]
 cl={str(x.get("id_client",x.get("_id"))):x for x in db["clienti"].find()}
 g={}
 for x in db["comenzi"].find():
  cid=str(x.get("client_id",""))
  val=fnum(x.get("valoare_totala_mdl",x.get("total_net_mdl",0)))
  if val>0: g.setdefault(cid,[]).append(val)
 for cid,vals in g.items():
  c=cl.get(cid,{})
  rows.append({"id":cid,"entitate":f"{c.get('nume','')} {c.get('prenume','')}","X1":len(vals),"X2":sum(vals),"Y":sum(vals)/len(vals),"KPI":"B2C"})
 comp={str(x.get("id_companie",x.get("_id"))):x for x in db["companii"].find()}
 g={}
 for x in db["comenzi_angro"].find():
  cid=str(x.get("companie_id",x.get("id_companie","")))
  val=fnum(x.get("valoare_totala_angro_mdl",x.get("total_net_mdl",0)))
  if val>0: g.setdefault(cid,[]).append(val)
 for cid,vals in g.items():
  c=comp.get(cid,{})
  rows.append({"id":cid,"entitate":c.get("denumire",""),"X1":len(vals),"X2":sum(vals),"Y":sum(vals)/len(vals),"KPI":"B2B"})
 return pd.DataFrame(rows)
def kpi_denormalizat():
 db=dbase("denormalizat")
 rows=[]
 for x in db["clienti_denormalizat"].find():
  nr=fnum(x.get("nr_comenzi",len(x.get("comenzi",[]))))
  total=fnum(x.get("valoare_totala_mdl",x.get("valoare_totala_comenzi_mdl",0)))
  if total==0 and isinstance(x.get("comenzi"),list): total=sum(fnum(c.get("valoare_totala_mdl",c.get("total_net_mdl",0))) for c in x.get("comenzi",[]))
  if nr>0 and total>0: rows.append({"id":x.get("id_client",x.get("_id")),"entitate":f"{x.get('nume','')} {x.get('prenume','')}","X1":nr,"X2":total,"Y":total/nr,"KPI":"B2C"})
 for x in db["companii_denormalizat"].find():
  nr=fnum(x.get("nr_comenzi_angro",len(x.get("comenzi_angro",[]))))
  total=fnum(x.get("valoare_totala_angro_mdl",x.get("valoare_totala_comenzi_angro_mdl",0)))
  if total==0 and isinstance(x.get("comenzi_angro"),list): total=sum(fnum(c.get("valoare_totala_angro_mdl",c.get("total_net_mdl",0))) for c in x.get("comenzi_angro",[]))
  if nr>0 and total>0: rows.append({"id":x.get("id_companie",x.get("_id")),"entitate":x.get("denumire",""),"X1":nr,"X2":total,"Y":total/nr,"KPI":"B2B"})
 return pd.DataFrame(rows)
def kpi_dwh(conn):
 if not create_engine or not quote_plus: return pd.DataFrame()
 try:
  eng=create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(conn)}")
  q1="SELECT CAST(nr_comenzi AS FLOAT) X1,CAST(valoare_totala_mdl AS FLOAT) X2,CAST(valoare_medie_comanda_mdl AS FLOAT) Y,CAST(id_client AS VARCHAR(100)) id,CAST(nume_client AS NVARCHAR(250)) entitate,'B2C' KPI FROM kpi.KPI_Clienti_B2C WHERE nr_comenzi IS NOT NULL AND valoare_totala_mdl IS NOT NULL AND valoare_medie_comanda_mdl IS NOT NULL"
  q2="SELECT CAST(nr_comenzi_angro AS FLOAT) X1,CAST(valoare_totala_angro_mdl AS FLOAT) X2,CAST(valoare_medie_comanda_angro_mdl AS FLOAT) Y,CAST(id_companie AS VARCHAR(100)) id,CAST(denumire_companie AS NVARCHAR(250)) entitate,'B2B' KPI FROM kpi.KPI_Companii_B2B WHERE nr_comenzi_angro IS NOT NULL AND valoare_totala_angro_mdl IS NOT NULL AND valoare_medie_comanda_angro_mdl IS NOT NULL"
  return pd.concat([pd.read_sql(q1,eng),pd.read_sql(q2,eng)],ignore_index=True)
 except Exception:
  return pd.DataFrame()
def regress(df,source):
 met=[]
 pred=[]
 models={}
 if df.empty: return pd.DataFrame(),pd.DataFrame(),{}
 for k in sorted(df["KPI"].dropna().unique()):
  d=df[df["KPI"]==k].dropna(subset=["X1","X2","Y"])
  d=d[(d["X1"]>0)&(d["X2"]>0)&(d["Y"]>0)]
  if len(d)<3: continue
  X=d[["X1","X2"]]
  y=d["Y"]
  model=LinearRegression().fit(X,y)
  p=model.predict(X)
  rmse=math.sqrt(mean_squared_error(y,p))
  mean=float(y.mean())
  med=float(y.median())
  p75=float(y.quantile(.75))
  p25=float(y.quantile(.25))
  met.append({"Sursa":source,"KPI":k,"R2":r2_score(y,p),"MAE":mean_absolute_error(y,p),"RMSE":rmse,"NRMSE":rmse/mean if mean else 0,"MeanY":mean,"MedianY":med,"P25":p25,"P75":p75,"Coef_X1":model.coef_[0],"Coef_X2":model.coef_[1],"Intercept":model.intercept_,"Nr":len(d)})
  z=d.copy()
  z["Sursa"]=source
  z["Y_prezis"]=p
  z["Eroare"]=z["Y"]-z["Y_prezis"]
  z["Eroare_abs"]=z["Eroare"].abs()
  pred.append(z)
  models[k]=model
 return pd.DataFrame(met),pd.concat(pred,ignore_index=True) if pred else pd.DataFrame(),models
def line_scatter(df,title_text):
 fig=go.Figure()
 for k in sorted(df["KPI"].unique()):
  d=df[df["KPI"]==k].sort_values("X2")
  fig.add_trace(go.Scatter(x=d["X2"],y=d["Y"],mode="markers",name=f"{k} real",text=d.get("entitate"),hovertemplate="%{text}<br>X2=%{x:.2f}<br>Y=%{y:.2f}<extra></extra>"))
  if len(d)>=3:
   model=LinearRegression().fit(d[["X1","X2"]],d["Y"])
   xx=d.copy().sort_values("X2")
   yy=model.predict(xx[["X1","X2"]])
   fig.add_trace(go.Scatter(x=xx["X2"],y=yy,mode="lines",name=f"{k} regresie"))
 fig.update_layout(title=title_text,xaxis_title="Valoare totală X2, MDL",yaxis_title="Valoare medie Y, MDL",hovermode="closest")
 return fig
def decision_from(m,p,kpi,scenario_y=None):
 r=m[m["KPI"]==kpi]
 if r.empty: return "Date insuficiente pentru decizie."
 r=r.iloc[0]
 p75=float(r["P75"])
 med=float(r["MedianY"])
 mean=float(r["MeanY"])
 r2=float(r["R2"])
 nrmse=float(r["NRMSE"])
 cx1=float(r["Coef_X1"])
 cx2=float(r["Coef_X2"])
 level="ridicat" if r2>=.85 else "mediu" if r2>=.7 else "scăzut"
 if scenario_y is None: scenario_y=mean
 zona="peste P75" if scenario_y>=p75 else "sub mediană" if scenario_y<med else "între mediană și P75"
 target="clienți B2C" if kpi=="B2C" else "companii B2B"
 action_high="menținere, fidelizare și oferte premium" if kpi=="B2C" else "prioritate la contracte, discount pe volum și negociere de prag minim"
 action_low="pachete promoționale și cross-sell pentru creșterea coșului" if kpi=="B2C" else "revizuire prag comandă angro și reducerea fragmentării comenzilor"
 action=action_high if scenario_y>=p75 else action_low
 return f"{kpi}: model cu nivel explicativ {level} (R²={r2:.3f}, NRMSE={nrmse:.3f}). P75={p75:.2f} MDL reprezintă pragul peste care intră primele 25% observații ca valoare medie; dacă prognoza depășește P75, entitatea intră în segment de valoare ridicată. Prognoza curentă este {zona}. Coeficientul X2={cx2:.3f} arată efectul valorii totale, iar X1={cx1:.3f} arată efectul frecvenței comenzilor. Decizie pentru {target}: {action}."
def decision_box(text):
 st.markdown(f"<div class='decision'>{text}</div>",unsafe_allow_html=True)

def plotly_layout(fig,title=None):
 if title:
  fig.update_layout(title={"text":title,"x":0.02,"xanchor":"left"})
 fig.update_layout(template="plotly_white",height=520,margin=dict(l=30,r=30,t=70,b=40),legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1),hovermode="closest")
 fig.update_xaxes(showgrid=True,gridcolor="rgba(0,0,0,.08)")
 fig.update_yaxes(showgrid=True,gridcolor="rgba(0,0,0,.08)")
 return fig
def e_bar_group(df,x,y,series,title,yname="Valoare"):
 if df.empty or x not in df.columns or y not in df.columns or series not in df.columns:
  return
 d=df.copy()
 d[y]=pd.to_numeric(d[y],errors="coerce")
 d=d.dropna(subset=[y])
 if d.empty:
  return
 fig=px.bar(d,x=x,y=y,color=series,barmode="group",text=d[y].round(3),title=title,color_discrete_sequence=px.colors.qualitative.Set2)
 fig.update_traces(textposition="outside",cliponaxis=False)
 fig.update_layout(yaxis_title=yname,xaxis_title=x)
 st.plotly_chart(plotly_layout(fig),use_container_width=True)
def e_bar_multi_rows(df,title):
 if df.empty:
  return
 d=df.copy()
 id_col="KPI" if "KPI" in d.columns else d.columns[0]
 cols=[c for c in ["media","p75","P75","MeanY","MedianY"] if c in d.columns]
 if not cols:
  return
 dd=d.melt(id_vars=[id_col],value_vars=cols,var_name="Indicator",value_name="MDL")
 dd["MDL"]=pd.to_numeric(dd["MDL"],errors="coerce")
 fig=px.bar(dd,x=id_col,y="MDL",color="Indicator",barmode="group",text=dd["MDL"].round(2),title=title,color_discrete_sequence=px.colors.qualitative.Set2)
 fig.update_traces(textposition="outside",cliponaxis=False)
 st.plotly_chart(plotly_layout(fig),use_container_width=True)
def e_reg_scatter(df,title):
 if df.empty or not set(["X1","X2","Y","KPI"]).issubset(df.columns):
  return
 d=df.dropna(subset=["X1","X2","Y","KPI"]).copy()
 d=d[(d["X2"]>0)&(d["Y"]>0)]
 if d.empty:
  return
 d["Mărime"]=np.clip(d["Y"].astype(float),d["Y"].quantile(.05),d["Y"].quantile(.95))
 fig=px.scatter(d,x="X2",y="Y",color="KPI",size="Mărime",hover_name="entitate" if "entitate" in d.columns else None,hover_data={"X1":True,"X2":":.2f","Y":":.2f","KPI":True},title=title,color_discrete_sequence=["#0b6ecb","#ff7f11","#1fa339"])
 for k in sorted(d["KPI"].dropna().unique()):
  z=d[d["KPI"]==k].sort_values("X2")
  if len(z)>=3:
   model=LinearRegression().fit(z[["X1","X2"]],z["Y"])
   z=z.copy();z["Y_regresie"]=model.predict(z[["X1","X2"]])
   fig.add_trace(go.Scatter(x=z["X2"],y=z["Y_regresie"],mode="lines",name=f"{k} regresie",line=dict(width=3)))
 fig.update_layout(xaxis_title="Valoare totală X2, MDL",yaxis_title="Valoare medie Y, MDL")
 st.plotly_chart(plotly_layout(fig),use_container_width=True)
def e_boxlike(df,title):
 if df.empty or "Y" not in df.columns:
  return
 d=df.copy()
 label_cols=[c for c in ["Model","Sursa","KPI"] if c in d.columns]
 if not label_cols:
  label_cols=["KPI"] if "KPI" in d.columns else []
 d["Etichetă"]=d[label_cols].astype(str).agg(" | ".join,axis=1) if label_cols else "Date"
 fig=px.box(d,x="Etichetă",y="Y",color="KPI" if "KPI" in d.columns else None,points="outliers",title=title,color_discrete_sequence=px.colors.qualitative.Set2)
 fig.update_layout(xaxis_tickangle=-20,yaxis_title="MDL")
 st.plotly_chart(plotly_layout(fig),use_container_width=True)
def e_scenarios(scen,r,title):
 if scen.empty or not set(["X2","Y_prognozat","Poziție","Scenariu"]).issubset(scen.columns):
  return
 d=scen.copy()
 d["Y_prognozat"]=pd.to_numeric(d["Y_prognozat"],errors="coerce").fillna(0).clip(lower=0)
 d["Mărime cerc"]=d["Y_prognozat"].abs().clip(lower=1)
 fig=px.scatter(d,x="X2",y="Y_prognozat",size="Mărime cerc",color="Poziție",text="Scenariu",hover_data={"X1":True,"X2":":.2f","Y_prognozat":":.2f","Poziție":True,"Mărime cerc":False},title=title,color_discrete_map={"Peste P75":"#1fa339","Între mediană și P75":"#ffb000","Sub mediană":"#e53935"})
 fig.add_hline(y=max(float(r.get("MeanY",0)),0),line_dash="dash",line_color="#777",annotation_text="media istorică")
 fig.add_hline(y=max(float(r.get("P75",0)),0),line_dash="dot",line_color="#1fa339",annotation_text="P75")
 fig.update_traces(textposition="top center")
 fig.update_layout(xaxis_title="Valoare totală X2, MDL",yaxis_title="Y prognozat, MDL")
 st.plotly_chart(plotly_layout(fig),use_container_width=True)
def e_donut(scen,title):
 if scen.empty or "Y_prognozat" not in scen.columns:
  return
 d=scen.copy()
 d["Valoare pentru grafic"]=pd.to_numeric(d["Y_prognozat"],errors="coerce").fillna(0).clip(lower=0)
 if d["Valoare pentru grafic"].sum()<=0:
  d["Valoare pentru grafic"]=1
 fig=px.pie(d,names="Scenariu",values="Valoare pentru grafic",hole=.58,title=title,color_discrete_sequence=px.colors.qualitative.Set2)
 fig.update_traces(textinfo="label+percent",hovertemplate="%{label}<br>%{value:.2f} MDL<br>%{percent}<extra></extra>")
 st.plotly_chart(plotly_layout(fig),use_container_width=True)
def e_real_pred(pp,title):
 if pp.empty or not set(["Y","Y_prezis"]).issubset(pp.columns):
  return
 d=pp.copy()
 d["Eroare_abs"]=pd.to_numeric(d.get("Eroare_abs",(d["Y"]-d["Y_prezis"]).abs()),errors="coerce").fillna(0).abs().clip(lower=1)
 fig=px.scatter(d,x="Y",y="Y_prezis",size="Eroare_abs",color="KPI" if "KPI" in d.columns else None,hover_name="entitate" if "entitate" in d.columns else None,title=title,color_discrete_sequence=px.colors.qualitative.Set2)
 maxv=float(pd.concat([d["Y"],d["Y_prezis"]]).max()) if len(d) else 1
 fig.add_trace(go.Scatter(x=[0,maxv],y=[0,maxv],mode="lines",name="linie ideală",line=dict(dash="dash",color="#1fa339",width=3)))
 fig.update_layout(xaxis_title="Y real",yaxis_title="Y prezis")
 st.plotly_chart(plotly_layout(fig),use_container_width=True)
def e_hist(values,title):
 vals=pd.Series(values).dropna().astype(float)
 if vals.empty:
  return
 fig=px.histogram(vals.to_frame(name="Eroare"),x="Eroare",nbins=20,title=title,color_discrete_sequence=["#ff7f11"])
 fig.update_layout(xaxis_title="Eroare",yaxis_title="Număr observații",showlegend=False)
 st.plotly_chart(plotly_layout(fig),use_container_width=True)

def safe_insert_many(collection,data):
 if not data:
  return 0
 try:
  r=collection.insert_many(data,ordered=False)
  return len(r.inserted_ids)
 except BulkWriteError as e:
  return int(e.details.get("nInserted",0))
 except DuplicateKeyError:
  return 0
def insert_normalized_batch(n):
 db=dbase("normalizat")
 run=now("BDN")
 branduri=["Nr.1","Casa Bună","Gusto","Fresh","EcoClean","DulceLux","Moldova"]
 cat=["CAT_ALIMENTE","CAT_BAUTURI","CAT_IGIENA","CAT_CAFEA","CAT_DULCIURI"]
 produse=[]
 clienti=[]
 companii=[]
 comenzi=[]
 linii=[]
 comenzi_angro=[]
 linii_angro=[]
 for i in range(max(20,n//20)):
  pid=f"{run}_PRD_{i}"
  produse.append({"_id":pid,"id_produs":pid,"cod_bare":f"CB{run[-12:]}{i:05d}","denumire":f"Produs {i} {random.choice(branduri)}","categorie_id":random.choice(cat),"brand_id":random.choice(branduri),"pret_mdl":round(random.uniform(8,850),2),"run_id":run})
 for i in range(max(35,n//10)):
  cid=f"{run}_CLI_{i}"
  clienti.append({"_id":cid,"id_client":cid,"nume":f"Client{i}","prenume":"Nr1","email":f"client{i}@nr1.md","telefon":"+3736"+str(random.randint(1000000,9999999)),"oras":"Chișinău","status":"activ","run_id":run})
 for i in range(max(8,n//60)):
  co=f"{run}_CMP_{i}"
  companii.append({"_id":co,"id_companie":co,"denumire":f"Companie {i} SRL","idno":str(random.randint(1000000000000,9999999999999)),"tip_activitate":random.choice(["restaurant","hotel","office","cafenea"]),"telefon":"+37322"+str(random.randint(100000,999999)),"email":f"b2b{i}@nr1.md","run_id":run})
 for i in range(max(45,n//6)):
  c=random.choice(clienti)
  cmd=f"{run}_CMD_{i}"
  vals=[]
  for j in range(random.randint(1,4)):
   p=random.choice(produse)
   q=random.randint(1,5)
   sub=round(q*p["pret_mdl"],2)
   vals.append(sub)
   linii.append({"_id":f"{run}_LIN_{i}_{j}","id_linie":f"{run}_LIN_{i}_{j}","comanda_id":cmd,"produs_id":p["id_produs"],"cantitate":q,"pret_unitar_mdl":p["pret_mdl"],"valoare_linie_mdl":sub,"run_id":run})
  comenzi.append({"_id":cmd,"id_comanda":cmd,"client_id":c["id_client"],"data_comanda":(datetime.now()-timedelta(days=random.randint(0,180))).strftime("%Y-%m-%d"),"valoare_totala_mdl":round(sum(vals),2),"status":"finalizată","tip_flux":"B2C","run_id":run})
 for i in range(max(8,n//60)):
  c=random.choice(companii)
  cmd=f"{run}_ACMD_{i}"
  vals=[]
  for j in range(random.randint(2,7)):
   p=random.choice(produse)
   q=random.randint(10,60)
   sub=round(q*p["pret_mdl"]*.9,2)
   vals.append(sub)
   linii_angro.append({"_id":f"{run}_ALIN_{i}_{j}","id_linie_angro":f"{run}_ALIN_{i}_{j}","comanda_angro_id":cmd,"produs_id":p["id_produs"],"cantitate":q,"pret_unitar_mdl":p["pret_mdl"],"valoare_linie_mdl":sub,"run_id":run})
  comenzi_angro.append({"_id":cmd,"id_comanda_angro":cmd,"companie_id":c["id_companie"],"data_comanda":(datetime.now()-timedelta(days=random.randint(0,180))).strftime("%Y-%m-%d"),"valoare_totala_angro_mdl":round(sum(vals),2),"status":"finalizată","tip_flux":"B2B","run_id":run})
 plan=[("produse",produse),("clienti",clienti),("companii",companii),("comenzi",comenzi),("linii_comanda",linii),("comenzi_angro",comenzi_angro),("linii_comenzi_angro",linii_angro)]
 total=0
 t=time.perf_counter()
 for c,data in plan:
  if data:
   ins=safe_insert_many(db[c],data)
   total+=ins
 denormalize_run(run)
 return {"run_id":run,"documente":total,"durata":time.perf_counter()-t}
def denormalize_run(run):
 nd=dbase("normalizat")
 dd=dbase("denormalizat")
 produse={x["id_produs"]:rowdoc(x) for x in nd["produse"].find({"run_id":run})}
 lin={}
 for x in nd["linii_comanda"].find({"run_id":run}):
  y=rowdoc(x);y["produs"]=produse.get(y.get("produs_id"));lin.setdefault(y.get("comanda_id"),[]).append(y)
 com={}
 for x in nd["comenzi"].find({"run_id":run}):
  y=rowdoc(x);y["linii"]=lin.get(y.get("id_comanda"),[]);com.setdefault(y.get("client_id"),[]).append(y)
 docs=[]
 for x in nd["clienti"].find({"run_id":run}):
  y=rowdoc(x);cs=com.get(y.get("id_client"),[]);total=sum(fnum(c.get("valoare_totala_mdl")) for c in cs);nr=len(cs);y.update({"comenzi":cs,"nr_comenzi":nr,"valoare_totala_mdl":round(total,2),"valoare_medie_comanda_mdl":round(total/nr,2) if nr else 0});docs.append(y)
 if docs: safe_insert_many(dd["clienti_denormalizat"],docs)
 lna={}
 for x in nd["linii_comenzi_angro"].find({"run_id":run}):
  y=rowdoc(x);y["produs"]=produse.get(y.get("produs_id"));lna.setdefault(y.get("comanda_angro_id"),[]).append(y)
 coma={}
 for x in nd["comenzi_angro"].find({"run_id":run}):
  y=rowdoc(x);y["linii"]=lna.get(y.get("id_comanda_angro"),[]);coma.setdefault(y.get("companie_id"),[]).append(y)
 docs=[]
 for x in nd["companii"].find({"run_id":run}):
  y=rowdoc(x);cs=coma.get(y.get("id_companie"),[]);total=sum(fnum(c.get("valoare_totala_angro_mdl")) for c in cs);nr=len(cs);y.update({"comenzi_angro":cs,"nr_comenzi_angro":nr,"valoare_totala_angro_mdl":round(total,2),"valoare_medie_comanda_angro_mdl":round(total/nr,2) if nr else 0});docs.append(y)
 if docs: safe_insert_many(dd["companii_denormalizat"],docs)
 if produse: safe_insert_many(dd["produse_denormalizat"],list(produse.values()))
def insert_denormalized_batch(n):
 dd=dbase("denormalizat")
 run=now("BDD")
 branduri=["Nr.1","Casa Bună","Gusto","Fresh","EcoClean"]
 produse=[]
 for i in range(max(20,n//20)):
  p=f"{run}_DPRD_{i}"
  produse.append({"_id":p,"id_produs":p,"cod_bare":f"CBD{run[-12:]}{i:05d}","denumire":f"Produs denorm {i}","categorie":random.choice(["Alimente","Băuturi","Igienă"]),"brand":random.choice(branduri),"pret_mdl":round(random.uniform(8,850),2),"cod_bare":f"CBD{run[-12:]}{i:05d}","promotii":[],"nr_promotii":0,"run_id":run})
 clienti=[]
 for i in range(max(35,n//8)):
  cid=f"{run}_DCLI_{i}"
  cs=[]
  for j in range(random.randint(1,3)):
   vals=[];lins=[]
   for z in range(random.randint(1,4)):
    p=random.choice(produse);q=random.randint(1,5);sub=round(q*p["pret_mdl"],2);vals.append(sub);lins.append({"produs":p,"cantitate":q,"valoare_linie_mdl":sub})
   cs.append({"id_comanda":f"{run}_DCMD_{i}_{j}","data_comanda":datetime.now().strftime("%Y-%m-%d"),"valoare_totala_mdl":round(sum(vals),2),"status":"finalizată","linii":lins})
  total=sum(c["valoare_totala_mdl"] for c in cs);nr=len(cs)
  clienti.append({"_id":cid,"id_client":cid,"nume":f"ClientD{i}","prenume":"Nr1","email":f"clientd{i}@nr1.md","telefon":"+3736"+str(random.randint(1000000,9999999)),"oras":"Chișinău","nr_comenzi":nr,"valoare_totala_mdl":round(total,2),"valoare_medie_comanda_mdl":round(total/nr,2),"comenzi":cs,"run_id":run})
 companii=[]
 for i in range(max(8,n//70)):
  cid=f"{run}_DCMP_{i}"
  cs=[]
  for j in range(random.randint(1,3)):
   vals=[];lins=[]
   for z in range(random.randint(2,7)):
    p=random.choice(produse);q=random.randint(10,60);sub=round(q*p["pret_mdl"]*.9,2);vals.append(sub);lins.append({"produs":p,"cantitate":q,"valoare_linie_mdl":sub})
   cs.append({"id_comanda_angro":f"{run}_DACMD_{i}_{j}","data_comanda":datetime.now().strftime("%Y-%m-%d"),"valoare_totala_angro_mdl":round(sum(vals),2),"status":"finalizată","linii":lins})
  total=sum(c["valoare_totala_angro_mdl"] for c in cs);nr=len(cs)
  companii.append({"_id":cid,"id_companie":cid,"denumire":f"CompanieD {i} SRL","idno":str(random.randint(1000000000000,9999999999999)),"tip_activitate":random.choice(["restaurant","hotel","office"]),"nr_comenzi_angro":nr,"valoare_totala_angro_mdl":round(total,2),"valoare_medie_comanda_angro_mdl":round(total/nr,2),"comenzi_angro":cs,"run_id":run})
 t=time.perf_counter()
 total=0
 for c,data in [("produse_denormalizat",produse),("clienti_denormalizat",clienti),("companii_denormalizat",companii)]:
  if data:
   ins=safe_insert_many(dd[c],data)
   total+=ins
 return {"run_id":run,"documente":total,"durata":time.perf_counter()-t}
def editable_doc(model,col):
 return {"normalizat":{"clienti":["nume","prenume","email","telefon","oras","status"],"companii":["denumire","tip_activitate","telefon","email"],"produse":["denumire","pret_mdl"],"comenzi":["valoare_totala_mdl","status"]},"denormalizat":{"clienti_denormalizat":["nume","prenume","email","telefon","oras"],"companii_denormalizat":["denumire","tip_activitate"],"produse_denormalizat":["denumire","categorie","brand","pret_mdl"]}}.get(model,{}).get(col,[])
def default_doc(model,col):
 if model=="normalizat" and col=="clienti":
  i=now("CLI");return {"_id":i,"id_client":i,"nume":"Nume","prenume":"Prenume","email":"client@nr1.md","telefon":"+37360000000","oras":"Chișinău","status":"activ"}
 if model=="normalizat" and col=="companii":
  i=now("CMP");return {"_id":i,"id_companie":i,"denumire":"Companie SRL","idno":"1000000000000","tip_activitate":"restaurant","telefon":"+37360000000","email":"b2b@nr1.md"}
 if model=="denormalizat" and col=="clienti_denormalizat":
  i=now("CLD");return {"_id":i,"id_client":i,"nume":"Nume","prenume":"Prenume","email":"client@nr1.md","telefon":"+37360000000","oras":"Chișinău","nr_comenzi":0,"valoare_totala_mdl":0,"valoare_medie_comanda_mdl":0,"comenzi":[]}
 if model=="denormalizat" and col=="companii_denormalizat":
  i=now("CPD");return {"_id":i,"id_companie":i,"denumire":"Companie SRL","idno":"1000000000000","tip_activitate":"restaurant","nr_comenzi_angro":0,"valoare_totala_angro_mdl":0,"valoare_medie_comanda_angro_mdl":0,"comenzi_angro":[]}
 if model=="denormalizat" and col=="produse_denormalizat":
  i=now("PRD");return {"_id":i,"id_produs":i,"denumire":"Produs","categorie":"Alimente","brand":"Nr.1","pret_mdl":10,"promotii":[],"nr_promotii":0}
 i=now("DOC");return {"_id":i,"denumire":"Document"}
def page_home():
 header("Dashboard managerial")
 a=kpi_normalizat();b=kpi_denormalizat()
 total=sum(dbase("normalizat")[c].count_documents({}) for c in N_COLS if c in dbase("normalizat").list_collection_names())+sum(dbase("denormalizat")[c].count_documents({}) for c in D_COLS if c in dbase("denormalizat").list_collection_names())
 c1,c2,c3,c4=st.columns(4)
 c1.metric("Documente",total);c2.metric("KPI normalizat",len(a));c3.metric("KPI denormalizat",len(b));c4.metric("Modele",2)
 frames=[]
 if not a.empty: a=a.copy();a["Model"]="Normalizat";frames.append(a)
 if not b.empty: b=b.copy();b["Model"]="Denormalizat";frames.append(b)
 if frames:
  data=pd.concat(frames,ignore_index=True)
  e_bar_group(data.groupby(["Model","KPI"]).agg(media=("Y","mean"),p75=("Y",lambda x:x.quantile(.75)),nr=("Y","count")).reset_index(),"KPI","media","Model","Media KPI pe model","MDL")
  e_boxlike(data,"Distribuția KPI: P25, mediană și P75")
  m,p,_=regress(data,"MongoDB")
  for k in sorted(m["KPI"].unique()): decision_box(decision_from(m,p,k))
def page_search():
 header("Search + CRUD")
 labels={f"{m}.{c}":(m,c) for m,c in all_sources()}
 q=st.text_input("Căutare",placeholder="client, produs, companie, telefon, ID, oraș")
 sel=st.multiselect("Colecții",list(labels),default=list(labels))
 res=search_docs(q,[labels[x] for x in sel],st.slider("Limită rezultate",20,500,120,20)) if sel else pd.DataFrame()
 if not res.empty: st.dataframe(res[[x for x in ["model","colectie","titlu","_id","id_client","id_companie","id_produs","denumire","nume","prenume","telefon","email"] if x in res.columns]],use_container_width=True)
 st.divider()
 model=st.selectbox("Model",["normalizat","denormalizat"])
 col=st.selectbox("Colecție",cols(model))
 op=st.radio("Operație",["CREATE","READ","UPDATE","DELETE","VALIDARE"],horizontal=True)
 database=dbase(model)
 if op=="CREATE":
  doc=default_doc(model,col)
  out={}
  for k,v in doc.items():
   if isinstance(v,(int,float)): out[k]=st.number_input(k,value=float(v))
   elif isinstance(v,list): out[k]=v
   else: out[k]=st.text_input(k,value=str(v))
  if st.button("Salvează"):
   t=time.perf_counter()
   try: database[col].insert_one(out);st.success(f"Inserat în {time.perf_counter()-t:.6f}s")
   except DuplicateKeyError: st.error("ID duplicat")
   except Exception as e: st.error(str(e))
 if op=="READ":
  docs=list(database[col].find().sort("_id",-1).limit(50));st.dataframe(pd.DataFrame([rowdoc(x) for x in docs]),use_container_width=True)
 if op in ["UPDATE","DELETE","VALIDARE"]:
  docs=list(database[col].find().sort("_id",-1).limit(50))
  if docs:
   names=[title(col,x) for x in docs]
   idx=st.selectbox("Document",range(len(docs)),format_func=lambda i:names[i])
   doc=docs[idx]
   st.json(rowdoc(doc),expanded=False)
   if op=="UPDATE":
    fields=editable_doc(model,col) or [k for k,v in doc.items() if k!="_id" and not isinstance(v,(dict,list))]
    field=st.selectbox("Câmp",fields)
    val=st.text_input("Valoare nouă",value=str(doc.get(field,"")))
    if st.button("Actualizează"):
     v=fnum(val) if isinstance(doc.get(field),float) or field.endswith("mdl") or field in ["cantitate","pret_mdl"] else val
     t=time.perf_counter();r=database[col].update_one({"_id":doc["_id"]},{"$set":{field:v}});st.success(f"Modificat: {r.modified_count} | {time.perf_counter()-t:.6f}s")
   if op=="DELETE" and st.button("Șterge documentul"):
    t=time.perf_counter();r=database[col].delete_one({"_id":doc["_id"]});st.success(f"Șters: {r.deleted_count} | {time.perf_counter()-t:.6f}s")
   if op=="VALIDARE":
    ok="_id" in doc
    msg="OK: documentul are identificator și structură JSON validă" if ok else "Lipsește _id"
    
    if ok:
     st.success(msg)
    else:
     st.error(msg)
def page_latency():
 header("Latență CRUD")
 if st.button("Testează latența pe ambele modele"):
  rows=[]
  for model,col in [("normalizat","clienti"),("denormalizat","clienti_denormalizat")]:
   database=dbase(model);doc=default_doc(model,col);doc["_id"]=now("LAT");doc["id_client"]=doc["_id"]
   t=time.perf_counter();database[col].insert_one(doc);rows.append({"Model":model,"Operație":"CREATE","Durata":time.perf_counter()-t})
   t=time.perf_counter();list(database[col].find({"_id":doc["_id"]}));rows.append({"Model":model,"Operație":"READ","Durata":time.perf_counter()-t})
   t=time.perf_counter();database[col].update_one({"_id":doc["_id"]},{"$set":{"telefon":"+37369999999"}});rows.append({"Model":model,"Operație":"UPDATE","Durata":time.perf_counter()-t})
   t=time.perf_counter();database[col].delete_one({"_id":doc["_id"]});rows.append({"Model":model,"Operație":"DELETE","Durata":time.perf_counter()-t})
  st.session_state.lat=pd.DataFrame(rows)
 df=st.session_state.get("lat",pd.DataFrame())
 if not df.empty:
  st.dataframe(df,use_container_width=True)
  e_bar_group(df,"Operație","Durata","Model","Comparație latență normalizat vs denormalizat","secunde")
  p=df.pivot_table(index="Operație",columns="Model",values="Durata").reset_index()
  p["Model mai rapid"]=p.apply(lambda r:"normalizat" if r.get("normalizat",999)<r.get("denormalizat",999) else "denormalizat",axis=1)
  p["Diferență_secunde"]=(p.get("normalizat",0)-p.get("denormalizat",0)).abs()
  st.dataframe(p,use_container_width=True)
  for _,r in p.iterrows(): decision_box(f"{r['Operație']}: modelul mai rapid este {r['Model mai rapid']}; diferența măsurată este {r['Diferență_secunde']:.6f}s. Această comparație indică impactul structurii documentelor asupra timpului de răspuns CRUD.")
def page_bigdata():
 header("Big Data + Spark")
 n=st.slider("Dimensiune lot",100,1200,500,50)
 c1,c2=st.columns(2)
 if c1.button("Inserare lot normalizat + sincronizare denormalizat"):
  try: st.success(str(insert_normalized_batch(n)))
  except Exception as e: st.error(str(e))
 if c2.button("Inserare lot denormalizat direct"):
  try: st.success(str(insert_denormalized_batch(n)))
  except Exception as e: st.error(str(e))
 src=st.selectbox("Sursă analizată",["normalizat","denormalizat"])
 data=kpi_normalizat() if src=="normalizat" else kpi_denormalizat()
 if not data.empty:
  s=data.groupby("KPI").agg(nr=("Y","count"),media=("Y","mean"),p75=("Y",lambda x:x.quantile(.75)),total=("X2","sum")).reset_index()
  st.dataframe(s,use_container_width=True)
  e_reg_scatter(data,"Relația dinamică X2-Y după loturile existente")
  e_bar_multi_rows(s,"Media și P75 pe flux")
  m,p,_=regress(data,f"MongoDB {src}")
  for k in sorted(m["KPI"].unique()): decision_box(decision_from(m,p,k))
def page_forecast():
 header("Prognoze NoSQL")
 model=st.selectbox("Model",["normalizat","denormalizat"])
 data=kpi_normalizat() if model=="normalizat" else kpi_denormalizat()
 m,p,models=regress(data,f"MongoDB {model}")
 if m.empty: st.warning("Date insuficiente");return
 st.dataframe(m,use_container_width=True)
 k=st.selectbox("KPI",sorted(m["KPI"].unique()))
 d=data[data["KPI"]==k]
 r=m[m["KPI"]==k].iloc[0]
 x1=st.slider("Număr comenzi X1",1.0,float(max(2,d["X1"].max()*2)),float(max(1,d["X1"].median())),1.0)
 x2=st.slider("Valoare totală X2",0.0,float(max(1000,d["X2"].max()*2)),float(d["X2"].median()),50.0)
 y_raw=float(models[k].predict(pd.DataFrame([{"X1":x1,"X2":x2}]))[0])
 y=max(y_raw,0)
 st.metric("Valoare medie prognozată",f"{y:.2f} MDL",f"{y-r['MeanY']:.2f} vs media")
 scen=pd.DataFrame([{"Scenariu":"actual","X1":x1,"X2":x2},{"Scenariu":"creștere X2 cu 10%","X1":x1,"X2":x2*1.1},{"Scenariu":"creștere X2 cu 25%","X1":x1,"X2":x2*1.25},{"Scenariu":"fragmentare: +1 comandă","X1":x1+1,"X2":x2}])
 scen["Y_prognozat_raw"]=models[k].predict(scen[["X1","X2"]])
 scen["Y_prognozat"]=pd.to_numeric(scen["Y_prognozat_raw"],errors="coerce").fillna(0).clip(lower=0)
 scen["Poziție"]=["Peste P75" if v>=r["P75"] else "Sub mediană" if v<r["MedianY"] else "Între mediană și P75" for v in scen["Y_prognozat"]]
 st.dataframe(scen,use_container_width=True)
 e_scenarios(scen,r,"Prognoze pe scenarii: cercuri dinamice X2-Y")
 e_donut(scen,"Pondere scenarii prognozate")
 best=scen.sort_values("Y_prognozat",ascending=False).iloc[0]
 worst=scen.sort_values("Y_prognozat").iloc[0]
 delta=float(best["Y_prognozat"]-worst["Y_prognozat"])
 decision_box(decision_from(m,p,k,y))
 decision_box(f"Scenariul cu impact maxim este '{best['Scenariu']}' cu {best['Y_prognozat']:.2f} MDL. Diferența față de scenariul minim '{worst['Scenariu']}' este {delta:.2f} MDL; această diferență indică potențialul comercial obținut prin modificarea valorii totale sau reducerea fragmentării comenzilor.")
 st.markdown(f"<div class='warn'><b>P75:</b> pragul de 75% arată nivelul sub care se află 75% dintre clienți/companii. Depășirea acestuia indică segment valoros și justifică acțiuni comerciale prioritare.</div>",unsafe_allow_html=True)
def page_mining():
 header("Data Mining Python")
 src=st.selectbox("Sursă",["normalizat","denormalizat"])
 data=kpi_normalizat() if src=="normalizat" else kpi_denormalizat()
 m,p,_=regress(data,f"MongoDB {src}")
 if m.empty: st.warning("Date insuficiente");return
 st.dataframe(m,use_container_width=True)
 k=st.selectbox("KPI",sorted(m["KPI"].unique()))
 pp=p[p["KPI"]==k]
 e_reg_scatter(data[data["KPI"]==k],f"Regresie {k}")
 e_real_pred(pp,"Real vs prezis")
 e_hist(pp["Eroare"],"Distribuția erorilor")
 decision_box(decision_from(m,p,k))
def page_compare():
 header("DWH vs MongoDB")
 conn=st.text_input("Conexiune SQL Server",SQL_CONN)
 if st.button("Rulează comparația"):
  frames=[];preds=[]
  for name,df in [("DWH_SQL_SERVER",kpi_dwh(conn)),("MONGODB_NORMALIZAT",kpi_normalizat()),("MONGODB_DENORMALIZAT",kpi_denormalizat())]:
   if not df.empty:
    m,p,_=regress(df,name);frames.append(m);preds.append(p)
  st.session_state.cmpm=pd.concat(frames,ignore_index=True) if frames else pd.DataFrame()
  st.session_state.cmpp=pd.concat(preds,ignore_index=True) if preds else pd.DataFrame()
 m=st.session_state.get("cmpm",pd.DataFrame());p=st.session_state.get("cmpp",pd.DataFrame())
 if not m.empty:
  st.dataframe(m,use_container_width=True)
  e_bar_group(m,"KPI","R2","Sursa","Comparație R² între modele","R²")
  e_bar_group(m,"KPI","NRMSE","Sursa","Comparație eroare relativă NRMSE","NRMSE")
  best=m.sort_values(["KPI","NRMSE"]).groupby("KPI").head(1)[["KPI","Sursa","R2","NRMSE","P75"]]
  st.dataframe(best,use_container_width=True)
  if not p.empty: e_reg_scatter(p,"Multi-series scatter: DWH și MongoDB")
  for k in sorted(m["KPI"].unique()):
   b=best[best["KPI"]==k].iloc[0]
   decision_box(f"{k}: cel mai stabil model după NRMSE este {b['Sursa']} (R²={b['R2']:.3f}, NRMSE={b['NRMSE']:.3f}). Comparația arată care sursă este mai potrivită pentru prognoză operațională, nu doar pentru raportare.")
def sidebar():
 logo=os.path.join(os.path.dirname(__file__),"assets","nr1_logo.png")
 if os.path.exists(logo): st.sidebar.image(logo,use_container_width=True)
 st.sidebar.markdown("<h1 style='color:white'>NR.1 Clienți</h1>",unsafe_allow_html=True)
 return st.sidebar.radio("Navigare",["Dashboard managerial","Search + CRUD","Latență CRUD","Big Data + Spark","Prognoze NoSQL","Data Mining Python","DWH vs MongoDB"])
def main():
 try: client()
 except Exception as e: st.error(f"MongoDB indisponibil: {e}");return
 page=sidebar()
 if page=="Dashboard managerial": page_home()
 elif page=="Search + CRUD": page_search()
 elif page=="Latență CRUD": page_latency()
 elif page=="Big Data + Spark": page_bigdata()
 elif page=="Prognoze NoSQL": page_forecast()
 elif page=="Data Mining Python": page_mining()
 elif page=="DWH vs MongoDB": page_compare()
if __name__=="__main__": main()
