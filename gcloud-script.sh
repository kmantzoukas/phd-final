#!/bin/bash

# Default paramters
rootFolder=/home/konstantinos/phd-final
bucket=gs://dataproc-staging-us-central1-658776204196-yztfnikr
workers=0

isClusterDefined=false
isUploadCaptorsDefined=false
isUploadServicesDefined=false

# Create Spark cluster of certain size i.e. with a specific number of worker nodes
function cluster() {
	gcloud dataproc clusters create spark-$1 \
	--region us-central1 \
	--subnet default \
	--zone us-central1-f \
	--master-machine-type n1-standard-1 \
	--master-boot-disk-size 20 \
	--num-workers $1 \
	--worker-machine-type n1-standard-1 \
	--worker-boot-disk-size 20 \
	--image-version 1.5-debian10 \
	--project phd-experiments-288211
}

# Upload Big Data services (jars) binaries
function services() {

	if [ ! -d "$1/code/AnonymizeData/target/scala-2.11" ]; then
	    cd $1/code/AnonymizeData && sbt clean package
	fi

	if [ ! -d "$1/code/PrepareData/target/scala-2.11" ]; then
	    cd $1/code/PrepareData && sbt clean package
	fi

	if [ ! -d "$1/code/ComputeAverages/target/scala-2.11" ]; then
	    cd $1/code/ComputeAverages && sbt clean package
	fi
	
	gsutil cp $1/code/AnonymizeData/target/scala-2.11/anonymizedata_*.jar $2/bda-services/
	gsutil cp $1/code/PrepareData/target/scala-2.11/preparedata_*.jar $2/bda-services/
	gsutil cp $1/code/ComputeAverages/target/scala-2.11/computeaverage_*.jar $2/bda-services/
}

#  Upload event captors to the cluster's nodes
function captors() {
	
	for instance in $3
	do
		gcloud compute ssh root@$instance --command='mkdir /home/kmantzoukas/captors' --zone us-central1-f
				
		gcloud compute scp \
		$1/DataIntegrityEverestEventCaptors/target/DataIntegrityEverestEventCaptors.jar \
		root@$instance:$2/captors \
		--zone us-central1-f
		
		gcloud compute scp \
		$1/DataAvailabilityEverestEventCaptors/target/DataAvailabilityEverestEventCaptors.jar \
		root@$instance:$2/captors \
		--zone us-central1-f

		gcloud compute scp \
		$1/DataPrivacyEverestEventCaptors/target/DataPrivacyEverestEventCaptors.jar \
		root@$instance:$2/captors \
		--zone us-central1-f
	done
}

for arg in "$@"
do
    case $arg in
        --cluster)
		workers=$2
		instances=($(gcloud compute instances list --filter=spark-$workers | awk '{print $1}' | tail -n +2))
        isClusterDefined=true
        shift
        ;;
        --services)
        isUploadServicesDefined=true
        shift
        ;;
        --captors)
        isUploadCaptorsDefined=true
        shift
        ;;
    esac
done

if $isClusterDefined ; then 
	cluster $workers 
fi

if $isUploadServicesDefined; then 
	services $rootFolder $bucket 
fi

if $isUploadCaptorsDefined; then
	captors $rootFolder '/home/kmantzoukas' $instances 
fi