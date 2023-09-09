@echo off

if "%~1"=="" (
    echo Usage: %0 ^<filename^>
    exit /b 1
)

docker-compose exec spark-master spark-submit ^
    --master spark://spark-master:7077 ^
    --conf "spark.log.level=ERROR" ^
    /transform/%1
