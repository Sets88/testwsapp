=========
testwsapp
=========

to run application you have to use credentials_example.env as an example to create credentials.env file
which is configuration file

there are two parts which should be running: websocket application and update assets service, to run everything locally you have to init database first::

    ./init_db.sh


after DB initialization finished, you can run websocket app with::

    ./run_ws.sh


and run update assets service:

    ./run_update.sh


