package cn.lgwen.flink;

import cn.lgwen.examples.LabelImage;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.tensorflow.Graph;
import org.tensorflow.Output;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * 2019/10/9
 * aven.wu
 * danxieai258@163.com
 */
public class ImagePredict {

    public static void main(String[] args) throws Exception{
        String modelDir = "G:\\deeplearn\\inception5h";
        String imageFile = "G:\\deeplearn\\images\\timg.jpg";
        Path path = Paths.get(imageFile);
        List<String> label = readAllLinesOrExit(Paths.get(modelDir, "imagenet_comp_graph_label_strings.txt"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<byte[]> imageStream = env.fromCollection(Arrays.asList(Files.readAllBytes(path)));
        DataStream<Tensor<Float>> tensorStream = imageStream.map(ImagePredict::constructAndExecuteGraphToNormalizeImage);
        DataStream<Integer> result = tensorStream.flatMap(new RichFlatMapFunction<Tensor<Float>, Integer>() {
            private transient Graph g;
            private transient Session session;

            @Override
            public void flatMap(Tensor<Float> floatTensor, Collector<Integer> collector) throws Exception {
                Tensor<Float> result =
                        session.runner().feed("input", floatTensor).fetch("output").run().get(0).expect(Float.class);
                    final long[] rshape = result.shape();
                    if (result.numDimensions() != 2 || rshape[0] != 1) {
                        throw new RuntimeException(
                                String.format(
                                        "Expected model to produce a [1 N] shaped tensor where N is the number of labels, instead it produced one with shape %s",
                                        Arrays.toString(rshape)));
                    }
                    int nlabels = (int) rshape[1];
                    float[] labelProbabilities = result.copyTo(new float[1][nlabels])[0];
                    int index = maxIndex(labelProbabilities);
                    collector.collect(index);
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                Path p = Paths.get(modelDir, "tensorflow_inception_graph.pb");
                g = new Graph();
                g.importGraphDef(Files.readAllBytes(p));
                session = new Session(g);
            }

            @Override
            public void close() throws Exception {
                super.close();
                session.close();
                g.close();
            }
        });
        result.map(x -> label.get(x)).print();

        env.execute();
    }


    private static List<String> readAllLinesOrExit(Path path) {
        try {
            return Files.readAllLines(path, Charset.forName("UTF-8"));
        } catch (IOException e) {
            System.err.println("Failed to read [" + path + "]: " + e.getMessage());
            System.exit(0);
        }
        return null;
    }

    private static int maxIndex(float[] probabilities) {
        int best = 0;
        for (int i = 1; i < probabilities.length; ++i) {
            if (probabilities[i] > probabilities[best]) {
                best = i;
            }
        }
        return best;
    }

    private static Tensor<Float> constructAndExecuteGraphToNormalizeImage(byte[] imageBytes) {
        try (Graph g = new Graph()) {
            GraphBuilder b = new GraphBuilder(g);
            // Some constants specific to the pre-trained model at:
            // https://storage.googleapis.com/download.tensorflow.org/models/inception5h.zip
            //
            // - The model was trained with images scaled to 224x224 pixels.
            // - The colors, represented as R, G, B in 1-byte each were converted to
            //   float using (value - Mean)/Scale.
            final int H = 224;
            final int W = 224;
            final float mean = 117f;
            final float scale = 1f;

            // Since the graph is being constructed once per execution here, we can use a constant for the
            // input image. If the graph were to be re-used for multiple input images, a placeholder would
            // have been more appropriate.
            final Output<String> input = b.constant("input", imageBytes);
            final Output<Float> output =
                    b.div(
                            b.sub(
                                    b.resizeBilinear(
                                            b.expandDims(
                                                    b.cast(b.decodeJpeg(input, 3), Float.class),
                                                    b.constant("make_batch", 0)),
                                            b.constant("size", new int[] {H, W})),
                                    b.constant("mean", mean)),
                            b.constant("scale", scale));
            try (Session s = new Session(g)) {
                // Generally, there may be multiple output tensors, all of them must be closed to prevent resource leaks.
                return s.runner().fetch(output.op().name()).run().get(0).expect(Float.class);
            }
        }
    }

}
