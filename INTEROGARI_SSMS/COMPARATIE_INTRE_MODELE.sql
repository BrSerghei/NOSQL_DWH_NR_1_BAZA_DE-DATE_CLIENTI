SELECT cod_kpi,denumire_kpi,sursa_model,AVG(valoare_kpi) AS valoare_medie
FROM kpi.KPI_Integrat
GROUP BY cod_kpi,denumire_kpi,sursa_model
ORDER BY cod_kpi,sursa_model;
GO