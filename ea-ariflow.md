Start airflow dev:
* ```breeze start-airflow --python 3.8 --backend postgres --additional-python-deps "Flask-Admin pydevd-pycharm~=211.7142.13"```

Clean dev docker images:
* ```breeze cleanup-image```

Postgres connection string:
* ```sql_alchemy_conn = postgresql+psycopg2://postgres:airflow@localhost:25433/airflow```

Test endpoint performance:
* ```ab -c 32 -n 1000 "http://localhost:8080/EASAS/rest/airflow/dag/vikingmalt_dag1/test_map/jobData?calcPeriod=2&calcTimestamp=1623391929"```
