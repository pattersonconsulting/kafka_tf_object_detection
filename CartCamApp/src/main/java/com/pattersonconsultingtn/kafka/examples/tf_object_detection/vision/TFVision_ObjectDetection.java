package com.pattersonconsultingtn.kafka.examples.tf_object_detection.vision;

// java stuff
import java.text.MessageFormat;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.InputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import javax.imageio.ImageIO;

// logging stuff
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

// TF Stuff

import org.tensorflow.Graph;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import org.tensorflow.TensorFlow;
import org.tensorflow.DataType;
import org.tensorflow.Graph;
import org.tensorflow.Output;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import org.tensorflow.TensorFlow;
import org.tensorflow.types.UInt8;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Tensor;
import org.tensorflow.framework.MetaGraphDef;
import org.tensorflow.framework.SignatureDef;
import org.tensorflow.framework.TensorInfo;
import org.tensorflow.types.UInt8;

import static object_detection.protos.StringIntLabelMapOuterClass.StringIntLabelMap;
import static object_detection.protos.StringIntLabelMapOuterClass.StringIntLabelMapItem;

import com.google.protobuf.TextFormat;

import com.pattersonconsultingtn.kafka.examples.tf_object_detection.vision.TFModelUtils;

import org.apache.commons.io.IOUtils;

/**

    run the class

    java -cp ./target/uber-kafka_tf_object_detection-1.0.0.jar com.pattersonconsultingtn.kafka.examples.tf_object_detection.vision.TFVision_ObjectDetection --label_map /Users/josh/Documents/workspace/PattersonConsulting/confluent/kafka_tf_object_detection/src/main/resources/mscoco_label_map.pbtxt.txt --image_file /tmp/image1.jpg --tf_model_file /Users/josh/Documents/PCT/ml_models/ssd_inception_v2_coco_2017_11_17/saved_model/



*/
public class TFVision_ObjectDetection  {

    private static long globalStartTime = 0;
    private static long globalEndTime = 0;

    static int imageWidth = 0, imageHeight = 0, imageChannels = 3;

    private static float[][] boxes = null;

    private static int maxObjects = 0; //null; //(int) scoresT.shape()[1];
    private static float[] scores = null; //scoresT.copyTo(new float[1][maxObjects])[0];
    private static float[] classes = null; //classesT.copyTo(new float[1][maxObjects])[0];

    public List<VisualObject> scanImageForObjects( String modelFile, String labelMapFile, String inputImageFile ) throws Exception, IOException {

        ArrayList<VisualObject> foundObjects = new ArrayList<VisualObject>();

        BufferedImage bimg = ImageIO.read( new File( inputImageFile ) );
        imageWidth          = bimg.getWidth();
        imageHeight         = bimg.getHeight();        

        System.out.println( "Input Image: " + inputImageFile );
        System.out.println( "Input width: " + imageWidth );
        System.out.println( "Input height: " + imageHeight );



      final String[] labels = TFModelUtils.loadLabels( labelMapFile ); //args[1]);
      try (SavedModelBundle model = SavedModelBundle.load( modelFile, "serve" )) {
        //printSignature( model );
        //for (int arg = 2; arg < args.length; arg++) {
          final String filename = inputImageFile; //[arg];
          List<Tensor<?>> outputs = null;
          //this.outputs = null;
          try (Tensor<UInt8> input = TFModelUtils.makeImageTensor( filename )) {
            outputs =
                model
                    .session()
                    .runner()
                    .feed("image_tensor", input)
                    .fetch("detection_scores")
                    .fetch("detection_classes")
                    .fetch("detection_boxes")
                    .run();
          }
          try (Tensor<Float> scoresT = outputs.get(0).expect(Float.class);
              Tensor<Float> classesT = outputs.get(1).expect(Float.class);
              Tensor<Float> boxesT = outputs.get(2).expect(Float.class)) {
            // All these tensors have:
            // - 1 as the first dimension
            // - maxObjects as the second dimension
            // While boxesT will have 4 as the third dimension (2 sets of (x, y) coordinates).
            // This can be verified by looking at scoresT.shape() etc.
            maxObjects = (int) scoresT.shape()[1];
            scores = scoresT.copyTo(new float[1][maxObjects])[0];
            classes = classesT.copyTo(new float[1][maxObjects])[0];
            boxes = boxesT.copyTo(new float[1][maxObjects][4])[0];

            // Print all objects whose score is at least 0.5.
            System.out.printf("* %s\n", filename);
            boolean foundSomething = false;

            for (int i = 0; i < scores.length; ++i) {
            
              if (scores[i] < 0.5) {
                continue;
              }

              int box_y = Math.round( boxes[ i ][ 0 ] * imageHeight );
              int box_x = Math.round( boxes[ i ][ 1 ] * imageWidth );
              int ymax = Math.round( boxes[ i ][ 2 ] * imageHeight );
              int xmax = Math.round( boxes[ i ][ 3 ] * imageWidth );

              int width = xmax - box_x;
              int height = ymax - box_y;


              VisualObject visObj = new VisualObject( box_x, box_y, width, height, scores[i], (int)classes[ i ], labels[ (int) classes[ i ] ] );
              foundObjects.add( visObj );

              foundSomething = true;
              System.out.printf("\tFound %-20s (score: %.4f)\n", labels[(int) classes[i]], scores[i]);
              System.out.println( "Box: " + boxes[ i ][ 0 ] + ", " + boxes[ i ][ 1 ] + ", " + boxes[ i ][ 2 ] + ", " + boxes[ i ][ 3 ] );
            
            }
            
            if (!foundSomething) {
              System.out.println("No objects detected with a high enough score.");
            } // if 
          
          } // try
        //} // for
      } // try

        return foundObjects;
    }


}
