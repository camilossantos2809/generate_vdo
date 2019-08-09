# SHELL := '/bin/bash'

run:
	clear
	watch -n 60 pipenv run python main.py

outdated:
	pipenv run pip list -o

git-log:
	git log --graph --oneline --decorate -n 20

git-log+:
	git log --stat --graph --decorate

# Gera relatório de cobertura de testes em formato HTML
coverage-html:
	clear
	pipenv run coverage run manage.py test -v 3 --tag=core
	pipenv run coverage html
	brave-browser htmlcov/index.html 2> /dev/null

# Gera relatório de cobertura de testes simplificado no terminal
coverage:
	clear
	pipenv run coverage run manage.py test
	pipenv run coverage report
