@ECHO OFF
docker compose -f .\docker-compose.yml -p local_project_aws_db up -d
:start_reset
SET choice=
SET /p choice=Do you want to reset the database? [Yes/No (N)]: 
IF NOT '%choice%'=='' SET choice=%choice:~0,1%
IF '%choice%'=='Y' GOTO yes_reset
IF '%choice%'=='y' GOTO yes_reset
IF '%choice%'=='N' GOTO end_reset
IF '%choice%'=='n' GOTO end_reset
IF '%choice%'=='' GOTO end_reset
ECHO "%choice%" is not valid
ECHO.
GOTO start_reset

:yes_reset
@echo off
setlocal enabledelayedexpansion
set "message=PLEASE WAIT 10 SECONDS "
set "dots=."
echo|set /p=!message!
for /L %%A in (1,1,10) do (
    <nul set /p "=.!dots:~0,%%A!"
    timeout 1 >nul
)
echo.
docker exec postgres_aws chmod o+x "/var/lib/postgresql/script/setup/"
docker exec postgres_aws chmod +x "/var/lib/postgresql/script/setup/start/run.sh"
ECHO chmod success
docker exec postgres_aws /bin/sh "/var/lib/postgresql/script/setup/start/run.sh"
ECHO ==== SUCCESS ====
GOTO end_reset

:end_reset
SET choice=
SET /p choice=Do you want to redeploy pgadmin4 server config? [Yes/No (N)]: 
IF NOT '%choice%'=='' SET choice=%choice:~0,1%
IF '%choice%'=='Y' GOTO yes_config
IF '%choice%'=='y' GOTO yes_config
IF '%choice%'=='N' GOTO end_config
IF '%choice%'=='n' GOTO end_config
IF '%choice%'=='' GOTO end_config
ECHO "%choice%" is not valid
ECHO.
GOTO end_reset

:yes_config
docker exec pgadmin4_aws /venv/bin/python3 /pgadmin4/setup.py --load-servers /var/lib/pgadmin/storage/user_user.com/script/setup/start/pgadmin_server_config.json --replace --user user@user.com
ECHO As a reminder db password is: pwd
GOTO end_config

:end_config
EXIT
