FROM kbase/sdkbase2:python
MAINTAINER KBase Developer
# -----------------------------------------
# In this section, you can install any system dependencies required
# to run your App.  For instance, you could place an apt-get update or
# install line here, a git checkout to download code, or run any other
# installation scripts.

# RUN apt-get update

RUN conda install python="3.7"

ENV ARANGO_VER=3.5.1
ENV ARANGO_VER_PRE=35

RUN curl -O https://download.arangodb.com/arangodb$ARANGO_VER_PRE/Community/Linux/arangodb3-linux-$ARANGO_VER.tar.gz \
    && tar -xf arangodb3-linux-$ARANGO_VER.tar.gz 

ENV ARANGO_EXE=/arangodb3-$ARANGO_VER/usr/sbin/arangod
ENV ARANGO_JS=/arangodb3-$ARANGO_VER/usr/share/arangodb3/js/

RUN pip install pipenv

# -----------------------------------------

COPY ./ /kb/module
RUN mkdir -p /kb/module/work
RUN chmod -R a+rw /kb/module

WORKDIR /kb/module

# really need a test build and a prod build. Not sure that's possible via sdk.
RUN pipenv install --system --deploy --ignore-pipfile --dev

RUN which python
RUN python --version

RUN cd test \
    && cp test.cfg.example test-sdk.cfg \
    && sed -i "s#^test.temp.dir=.*#test.temp.dir=temp_test_dir#" test-sdk.cfg \
    && sed -i "s#^test.arango.exe.*#test.arango.exe=$ARANGO_EXE#" test-sdk.cfg \
    && sed -i "s#^test.arango.js.*#test.arango.js=$ARANGO_JS#" test-sdk.cfg \
    && cat test-sdk.cfg

RUN make all

ENTRYPOINT [ "./scripts/entrypoint.sh" ]

CMD [ ]
