package com.pattersonconsultingtn.kafka.examples.tf_object_detection.vision;

public class VisualObject {

    public String label = "";
    private double confidence;
    private int predictedClass = -1;

    private int left;
    private int top;
    private int width;
    private int height;
    
    

	public VisualObject( int left, int top, int width, int height, double confidence, int classIndex, String label_txt ) {
        
        this.label = label_txt;
        this.left = left;
        this.top = top;
        this.width = width;
        this.height = height;
        this.confidence = confidence;
        this.predictedClass = classIndex;
    }
/*
	public double getCenterX() {
        return centerX;
    }

	public double getCenterY() {
        return centerY;
    }
*/

	public int getWidth() {
        return width;
    }

	public int getHeight() {
        return height;
    }


	public int getPredictedClass() {
        return predictedClass;
    }

	public double getConfidence() {
        return confidence;
    }

    public int getLeft() {
    	return left; //centerX - width / 2.0;
    }

    public int getTop() {
    	return top; //centerY - height / 2.0;
    }

/*
    public double[] getTopLeftXY() {

        return new double[]{ centerX - width / 2.0, centerY - height / 2.0};

    }

    public double[] getBottomRightXY(){

        return new double[]{ centerX + width / 2.0, centerY + height / 2.0};

    }
*/

}