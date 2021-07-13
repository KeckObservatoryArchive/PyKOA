#  All Docker images have to be built 'FROM' something

FROM debian:latest

#  This is the way you set environment variables to be use in the running container

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH /opt/conda/bin:$PATH


# 'RUN' directives modify the image in the same way running these commands would
#  change an OS instance.  Only here we are modifying what will be installed in
#  the saved Docker image.  We start with a few basic utilities that don't come 
#  with baseline debian.  The reason for all the continuation lines is that 
#  every  RUN creates another layer and uses up more disk space.

RUN apt update --fix-missing && \
    apt install -y wget bzip2 git && \
    apt install -y curl grep sed && \
    apt install -y vim && \
    apt install -y build-essential


#  Installing Anaconda gives us a bunch of tools (in particular Jupyter 
#  and Astropy) that will let us better interact with our data.

RUN wget --quiet https://repo.anaconda.com/archive/Anaconda3-2019.07-Linux-x86_64.sh -O ~/anaconda.sh && \
    /bin/bash ~/anaconda.sh -b -p /opt/conda && \
    rm ~/anaconda.sh

RUN pip install pykoa

#  Finally, for this container we want the default application to just be 
#  a shell so we can be logged-in as soon as we start the container.  
#  Other containers often default to running a web server, etc.

WORKDIR /work

CMD /bin/sh

