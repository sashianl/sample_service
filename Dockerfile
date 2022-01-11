FROM python:3.7
LABEL org.opencontainers.image.authors="KBase Developer"

# Install dockerize. It is not available through package managers.
# TODO: Switch to current upatream dockerize, which now has support 
# for KBase use cases; it is actively supported, and has additional fixes
# and improvements over the last couple of years since we forked it.
ENV DOCKERIZE_VERSION v0.6.1
RUN curl -o dockerize.tar.gz \
    https://raw.githubusercontent.com/kbase/dockerize/master/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && \
    tar -C /usr/local/bin -xvzf dockerize.tar.gz && \
    rm dockerize.tar.gz

ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Install Python dependencies
RUN mkdir -p /kb/tmp
COPY requirements.txt /kb/tmp
COPY Pipfile /kb/tmp
COPY Pipfile.lock /kb/tmp
RUN cd /kb/tmp && \
    pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pipenv install --system --deploy --ignore-pipfile --dev

# Install service code
COPY ./ /kb/module
WORKDIR /kb/module
RUN rm -rf /kb/tmp && \
    mkdir -p /kb/module/work && \
    chmod -R a+rw /kb/module 

# Entrypoint script accepts single-word arguments in CMD to invoke special
# modes, or leave empty to start the production server. 
# TODO: most services have dockerize in the dockerfile, have it invoke 
# the entrypoint.
ENTRYPOINT [ "./scripts/entrypoint.sh" ]
CMD [ ]
