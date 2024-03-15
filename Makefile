shell: build
	docker run -p 3334:3306 --privileged --rm -ti -v "$(shell pwd)":/app brmzkw/pgfuse /bin/bash

build:
	docker build -t brmzkw/pgfuse .