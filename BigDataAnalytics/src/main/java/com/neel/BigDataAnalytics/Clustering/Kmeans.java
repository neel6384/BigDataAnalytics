package com.neel.BigDataAnalytics.Clustering;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;

public class Kmeans {

	public static void main(String args[]) throws Exception {
		String inputDir = "hdfs://localhost:54310/Classification/20_newgroups_vectors/";
		//Path inputPath = new Path(inputDir);

		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:54310/");
		
		String vectorsFolder = inputDir + "/tfidf-vectors";
		Path samples = new Path(vectorsFolder + "/part-r-00000");
		Path output = new Path("hdfs://localhost:54310/Clustering/");
		HadoopUtil.delete(conf, output);
		DistanceMeasure measure = new CosineDistanceMeasure();

		Path clustersIn = new Path(output, "random-seeds");
		RandomSeedGenerator.buildRandom(conf, samples, clustersIn, 3, measure);
		KMeansDriver.run(samples, clustersIn, output, measure, 0.01, 10, true,
				0.0, true);

		List<List<Cluster>> Clusters = ClusterHelper.readClusters(conf, output);

		for (Cluster cluster : Clusters.get(Clusters.size() - 1)) {
			System.out.println("Cluster id: " + cluster.getId() + " center: "
					+ cluster.getCenter().asFormatString());
		}
	}
}
