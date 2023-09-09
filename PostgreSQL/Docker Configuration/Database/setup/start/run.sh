#!/bin/sh
cd /var/lib/postgresql/script/setup && psql -U postgres < global_script.sql
