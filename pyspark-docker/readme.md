# How to run

- Install Docker
- Go cd to the folder containing docker-compose.yml
- Run command `docker-compose up -d`
- Every Python script that is found in transform will be copied into the dockers
- If you are in Windows you can run `sparkrun.bat filename.py` it will execute the full command to run your pyspark code
- If you are in Mac/Linux based, you should be in bash terminal and run these 2 commands:
  - `chmod +x sparkrun.sh` to make the sparkrun.sh executable
  - `/bin/sh sparkrun.sh filename.py`

# To test everything 
Run this command
on Mac/Linux based
- `/bin/sh sparkrun.sh test.py`
On windows
- `sparkrun.battest.py`