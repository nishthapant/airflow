FROM apache/airflow:2.7.1

USER airflow

# Install dbt and other packages under the airflow user, using --user
RUN pip install --user dbt-core dbt-bigquery \
 && pip install --user apache-airflow-providers-fab \
 && pip install --user "apache-airflow-providers-openlineage>=1.8.0" \
 && pip install --user "protobuf==3.20.*"

# Add the user-level bin to the PATH
ENV PATH="/home/airflow/.local/bin:$PATH"
