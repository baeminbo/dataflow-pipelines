FROM gcr.io/dataflow-templates-base/java11-template-launcher-base:latest

ARG WORKDIR=/dataflow/template
RUN mkdir -p $WORKDIR
WORKDIR $WORKDIR

COPY java-pipeline-python-transform-bundled-1.0.0.jar ./
COPY mytransforms-1.0.0.tar.gz ./

# Without python3-venv, it fails to expand PythonExternalTransform in laucncher
# VM. The bootstrap_beam_venv.py fails to create a virtual environment.
RUN apt-get update &&  \
    apt-get install -y python3 python3-pip python3-venv

RUN pip install apache-beam[gcp]==2.45.0

ENV FLEX_TEMPLATE_JAVA_MAIN_CLASS=baeminbo.MyJavaPipeline
ENV FLEX_TEMPLATE_JAVA_CLASSPATH=java-pipeline-python-transform-bundled-1.0.0.jar

ENTRYPOINT ["/opt/google/dataflow/java_template_launcher"]
