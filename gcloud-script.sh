#!/bin/bash
captors=/media/konstantinos/Windows/Users/Administrator/phd-final/code/captors
instances=($(gcloud compute instances list --filter=spark-$numOfNodes | awk '{print $1}' | tail -n +2))
user=root
bdaservices=/media/konstantinos/Windows/Users/Administrator/phd-final/code
home=/home/kmantzoukas
gsbucketurl=gs://dataproc-staging-us-central1-658776204196-yztfnikr

# Default paramters
createCluster=false
numOfNodes=0
copyCaptors=false
copyServices=false
copyCaptors=false

# Create Spark cluster of certain size i.e. number of worker nodes
function createSparkCluster() {
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
function uploadBigDataServices() {
	gsutil cp $1/AnonymizeData/target/scala-2.11/anonymizedata_2.11-0.1.0-SNAPSHOT.jar $2/bda-services/
	gsutil cp $1/PrepareData/target/scala-2.11/preparedata_2.11-0.1.0-SNAPSHOT.jar $2/bda-services/
	gsutil cp $1/ComputeAverages/target/scala-2.11/computeaverage_2.11-0.1-SNAPSHOT.jar $2/bda-services/
}

#  Upload event captors to the cluster's nodes
function uploadEventCaptors() {
	
	for instance in "${instances[@]}"
	do
		echo -e "\nCreating folder $instance@/home/kmantzoukas/captors"
		gcloud compute ssh root@$instance --command='mkdir /home/kmantzoukas/captors' --zone us-central1-f
		echo -e "\nCopying data integrity event captor on $instance"
		gcloud compute scp $1/DataIntegrityEverestEventCaptors/target/DataIntegrityEverestEventCaptors.jar $user@$instance:$2/captors --zone us-central1-f
		echo -e "\nCopying data availability event captor"
		gcloud compute scp $1/DataAvailabilityEverestEventCaptors/target/DataAvailabilityEverestEventCaptors.jar $user@$instance:$2/captors --zone us-central1-f
		echo -e "\nCopying data privacy event captor"
		gcloud compute scp $1/DataPrivacyEverestEventCaptors/target/DataPrivacyEverestEventCaptors.jar $user@$instance:$2/captors --zone us-central1-f
	done
}

for arg in "$@"
do
    case $arg in
        --cluster)
		numOfNodes=$2
        createCluster=true
        shift
        ;;
        --captors)
        copyCaptors=true
        shift
        ;;
        --bda)
        copyServices=true
        shift
        ;;
    esac
done

if $createCluster ; then 
	createSparkCluster $numOfNodes 
fi

if $copyServices; then 
	uploadBigDataServices $bdaservices $gsbucketurl 
fi

if $copyCaptors; then 
	uploadEventCaptors $captors $home 
fi