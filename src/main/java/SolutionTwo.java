import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * DistCp
 */
public class SolutionTwo {
    private static final SparkConf sparkConf;
    private static final JavaSparkContext javaSparkContext;
    private static final Configuration configuration;

    static {
        sparkConf = new SparkConf().setAppName("DistCp").setMaster("local");
        javaSparkContext = new JavaSparkContext(sparkConf);
        configuration = javaSparkContext.hadoopConfiguration();
    }

    public static void main(String [] args) throws IOException {
        String sourceRootPathStr = args[0];
        String targetRootPathStr = args[1];

        JavaRDD<String> sourceFileListRDD = getSourceFileLists(sourceRootPathStr, targetRootPathStr);
        sourceFileListRDD.foreachPartition(sourceFileIterator -> {
            FileSystem sourceFileSystem = new Path(sourceRootPathStr).getFileSystem(configuration);
            FileSystem targetFileSystem = new Path(targetRootPathStr).getFileSystem(configuration);
            while(sourceFileIterator.hasNext()) {
                String sourceFilePath = sourceFileIterator.next();
                Path sourceFileRelativePath = new Path(new Path(sourceRootPathStr).toUri().relativize(new Path(sourceFilePath).toUri()));
                Path targetPath = new Path(targetRootPathStr, sourceFileRelativePath);
                InputStream sourceInputStream = sourceFileSystem.open(new Path(sourceFilePath));
                FSDataOutputStream fsDataOutputStream = targetFileSystem.create(targetPath, true);
                IOUtils.copy(sourceInputStream, fsDataOutputStream);
            }
        });
    }

    private static JavaRDD<String> getSourceFileLists(String sourceRootPathStr, String targetRootPathStr) throws IOException {
        Path sourceRootPath = new Path(sourceRootPathStr);
        Path targetRootPath = new Path(targetRootPathStr);
        FileSystem sourceFileSystem = sourceRootPath.getFileSystem(configuration);
        FileSystem targetFileSystem = targetRootPath.getFileSystem(configuration);
        RemoteIterator<LocatedFileStatus> iterator = sourceFileSystem.listFiles(sourceRootPath, true);
        Set<Path> distinctDirPaths = new HashSet<>();
        List<String> fileList = new ArrayList<>();
        while(iterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = iterator.next();
            Path filePath = locatedFileStatus.getPath();
            distinctDirPaths.add(filePath.getParent());
            fileList.add(filePath.toString());
        }
        distinctDirPaths.remove(sourceRootPath);
        for(Path distinctDirPath : distinctDirPaths) {
            String sourceChildrenDirRelativePathStr = sourceRootPath.toUri().relativize(distinctDirPath.toUri()).toString();
            targetFileSystem.mkdirs(new Path(targetRootPath, sourceChildrenDirRelativePathStr), new FsPermission(FsAction.ALL, FsAction.READ, FsAction.READ));
        }
        return javaSparkContext.parallelize(fileList, 4);
    }

}
