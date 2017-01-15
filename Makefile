init:
	pip3 install virtualenv;
	virtualenv .ve --no-site-packages
	.ve/bin/pip install -r requirements.txt

run:
    .ve/bin/python src/run.py runserver;