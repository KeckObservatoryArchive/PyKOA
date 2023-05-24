[webserver]
DBMS=pgsql
TAP_WORKDIR=/work/mihseh_9010
TAP_WORKURL=/workspace/mihseh_9010
HTTP_PORT=9010
COOKIENAME=KOA9010
CGI_PGM=/TAP

ACCESS_TBL=koa_access
USERS_TBL=koa_users_mk
DATAPATH=/koadata/ops
CALIBDIR=/koadata/ops/koadata7/KOACalib
FILEID=koaid
ACCESSID=semid
PROPFILTER=koa
RACOL=RA
DECCOL=DEC

# Spatial indexing settings
ADQL_MODE=HTM
ADQL_LEVEL=7
ADQL_XCOL=x
ADQL_YCOL=y
ADQL_ZCOL=z
ADQL_COLNAME=spt_ind
ADQL_ENCODING=BASE4

#
# KOA download log file suffix: 
# download_file = instrument.download_suffix
# download_dir = workdir + /log/
#
DOWNLOAD_LOG_DIR=/work/mihseh_9010/logs
DOWNLOAD_LOG_SUFFIX=download.log
LEV1_DOWNLOAD_LOG_SUFFIX=lev1.download.log

HTTP_URL=http://vmkoadev.ipac.caltech.edu

[oracle]
Driver=/usr/lib/oracle/12.2/client64/lib/libsqora.so.12.1
Description=Oracle 12g ODBC driver

ServerName=koaops1
UserID=koa_tap
Password=anHSBK6dgw

[pgsql]
HostName=vmnexscidb5
UserName=koa_tap
Password=koa_tap_123
DataBase=nxitd


#UserName=tap_schema
#Password=tap_schema_123
#DataBase=nxitd

#ServerName=koadev1
#UserID=koa_tap
#Password=anHSBK6dgw

