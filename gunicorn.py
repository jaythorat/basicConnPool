# from gunicorn.workers.ggevent import GeventWorker
# import multiprocessing
# import os
bind = "0.0.0.0:1234"
# bind = "unix:/home/jay/codebase/ooefWebsite/ooefWebsite/webapp.sock"
workers = 13
# workers = multiprocessing.cpu_count() * 2 + 1
# max_requests = 50
reload = True
# worker_class = GeventWorker
