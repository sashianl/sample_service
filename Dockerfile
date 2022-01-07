FROM python:3.7
LABEL org.opencontainers.image.authors="KBase Developer"

# TODO: Switch to current dockerize, which has support for KBase use cases now,
# but is actively supported -- ours isn't.
ENV DOCKERIZE_VERSION v0.6.1
RUN \
    curl -o dockerize.tar.gz \
    https://raw.githubusercontent.com/kbase/dockerize/master/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && \
    tar -C /usr/local/bin -xvzf dockerize.tar.gz && \
    rm dockerize.tar.gz

ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

COPY ./ /kb/module
WORKDIR /kb/module

RUN mkdir -p /kb/module/work \
    && chmod -R a+rw /kb/module \
    && pip install --upgrade pip \
    && pip install -r requirements.txt \
    && pipenv install --system --deploy --ignore-pipfile --dev

ENTRYPOINT [ "./scripts/entrypoint.sh" ]

CMD [ ]
