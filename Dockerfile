FROM python:3.7
MAINTAINER KBase Developer
# -----------------------------------------

ENV DOCKERIZE_VERSION v0.6.1

RUN \
    curl -o dockerize.tar.gz \
    https://raw.githubusercontent.com/kbase/dockerize/master/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && \
    tar -C /usr/local/bin -xvzf dockerize.tar.gz && \
    rm dockerize.tar.gz

ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN pip install pipenv

# -----------------------------------------
COPY Pipfile /tmp/Pipfile
COPY Pipfile.lock /tmp/Pipfile.lock
RUN cd /tmp && pipenv install --system --deploy --ignore-pipfile --dev

COPY ./ /kb/module
RUN mkdir -p /kb/module/work
RUN chmod -R a+rw /kb/module

WORKDIR /kb/module

# really need a test build and a prod build. Not sure that's possible via sdk.
# RUN pipenv install --system --deploy --ignore-pipfile --dev

ENTRYPOINT [ "./scripts/entrypoint.sh" ]

CMD [ ]
