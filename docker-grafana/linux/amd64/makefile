IMAGE := csullivan9999/nats-surveyor-grafana
TAG := v0.0.1-alpha1
 
build:
	$(info Make: Building "$(TAG)" tagged images.)
	cp -R ../../../docker-compose/grafana/provisioning .
	@docker build -t $(IMAGE):$(TAG) .
	@docker tag $(IMAGE):$(TAG) $(IMAGE):latest

push:
	$(info Make: Pushing "$(TAG)" tagged image.)
	@docker push $(IMAGE):$(TAG)
	@docker push $(IMAGE):latest
