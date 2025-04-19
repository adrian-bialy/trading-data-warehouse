lint:
	pylint --max-line-length=120 --fail-under=8 src/tdw tests

flake:
	flake8 --max-line-length 120 src/tdw tests

black:
	black --line-length 120 src/tdw tests

test:
	pytest --maxfail=0 --disable-warnings -q

precommit:
	make black lint flake test
