FROM golang:alpine

ADD . /go/src/github.com/nearmap/cvmanager
RUN go install github.com/nearmap/cvmanager

RUN rm -r /go/src/github.com/nearmap/cvmanager

VOLUME /go/src

# TODO: this is dodgy it expects k8s files to always be available from runtime directory
# need to package the yaml n version file using tool chains properly
RUN mkdir -p /cvmanager
ADD ./k8s /cvmanager/k8s/
ADD version /cvmanager/

WORKDIR /cvmanager

EXPOSE 2019

ENTRYPOINT ["cvmanager"]
