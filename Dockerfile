FROM java:openjdk-8-jdk

ENV DBMAINTAIN_VERSION=2.7.1

COPY dbmaintain/target/dbmaintain-${DBMAINTAIN_VERSION}.jar /lib/
RUN touch prescriptsqlpus.sql
RUN touch postscriptsqlpus.sql
COPY docker/entrypoint.sh /
ENTRYPOINT ["/entrypoint.sh"]
