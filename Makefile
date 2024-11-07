reports:
	mkdir -p reports
	docker build -t my_pyspark .
	docker run --rm \
		-v ./scripts:/opt/spark/work-dir/scripts \
		-v ./raw:/opt/spark/work-dir/raw \
		-v ./reports:/opt/spark/work-dir/reports \
		-u ${UID}:${GID} \
		my_pyspark /opt/spark/bin/spark-submit /opt/spark/work-dir/scripts/kpi.py

dashboard:
	docker build -f Dockerfile.streamlit -t streamlit .
	docker run -p 8501:8501 \
		-v ./scripts:/app/scripts \
		-v ./reports:/app/reports \
	 streamlit

# Always run, usefull for development
.PHONY: reports