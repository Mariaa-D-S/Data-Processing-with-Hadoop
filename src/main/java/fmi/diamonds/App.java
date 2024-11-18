package fmi.diamonds;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSlider;
import javax.swing.JSpinner;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

/**
 * Hello world!
 */
public class App extends JFrame {
	
	private JTextArea result = new JTextArea();
	private JComboBox<String> options = new JComboBox<>(new String[]{"Average Price", "Count with carats range"});
	private JSpinner min;
	private JSpinner max;
    public static void main(String[] args) {
    	App form = new App();
    }
    
    public App() {
		init();
	}
    private void init() {
    	JPanel panel = new JPanel();
    	JTextField quality = new JTextField();
    	JLabel qualityLabel = new JLabel("Diamond Quality:");
    	JTextField color = new JTextField();
    	JLabel colorLabel = new JLabel("Color:");
    	JTextField clarity = new JTextField();
    	JLabel clarityLabel = new JLabel("Clarity:");
    	options.setSelectedIndex(0);
    	min = new JSpinner(new SpinnerNumberModel(0, 0, 100, 0.01));
    	max = new JSpinner(new SpinnerNumberModel(0, 0, 100, 0.01));
    	JButton searchButton = new JButton("Search");
    	JScrollPane scroller = new JScrollPane(result);
    	
    	panel.setLayout(null);
    	
    	panel.add(qualityLabel);
    	panel.add(quality);
    	panel.add(colorLabel);
    	panel.add(color);
    	panel.add(clarityLabel);
    	panel.add(clarity);
    	panel.add(options);
    	panel.add(min);
    	panel.add(max);
    	panel.add(searchButton);
    	//panel.add(result);
    	panel.add(scroller);
    	
    	//500 800
    	qualityLabel.setBounds(20, 80, 150, 30);
    	quality.setBounds(160, 80, 300, 30);
    	colorLabel.setBounds(20, 120, 150, 30);
    	color.setBounds(160, 120, 300, 30);
    	clarityLabel.setBounds(20, 160, 150, 30);
    	clarity.setBounds(160, 160, 300, 30);
    	options.setBounds(180, 220, 200, 30);
    	min.setBounds(160, 260, 100, 30);
    	min.setVisible(false);
    	max.setBounds(300, 260, 100, 30);
    	max.setVisible(false);
    	searchButton.setBounds(200, 300, 100, 30);
    	//result.setBounds(50, 350, 380, 350);
    	scroller.setBounds(50, 350, 380, 350);
    	
    	options.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
            	if(options.getSelectedIndex() == 1) {
                min.setVisible(true);
                max.setVisible(true);
            	}else {
            		min.setVisible(false);
                max.setVisible(false);
            	}
            }
        });
    	
    	
    	setSize(500,800);
    	setVisible(true);
    	add(panel);
    	
    	searchButton.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				startHadoop(quality.getText(), color.getText(), clarity.getText());
				
			}
		});
    }

    protected void startHadoop(String filter1, String filter2, String filter3) {
    	Configuration conf = new Configuration();
    	Path inputPath = new Path("hdfs://127.0.0.1:9000/input/20.csv");
    	Path outputPath = new Path("hdfs://127.0.0.1:9000/result");
    	
    	JobConf job = new JobConf(conf, App.class);
    	
    	job.set("filter1", filter1);
    	job.set("filter2", filter2);
    	job.set("filter3", filter3);
    	
    	if (options.getSelectedIndex() == 0) {
    		job.setMapperClass(PriceMapper.class);
        	job.setReducerClass(PriceReducer.class);
        	job.setOutputKeyClass(Text.class);
        	job.setMapOutputValueClass(DoubleWritable.class);
    	} else if (options.getSelectedIndex() == 1) {
    		job.setMapperClass(CountMapper.class);
    		job.setReducerClass(CountReducer.class);
    		job.setOutputKeyClass(Text.class);
    		job.setMapOutputValueClass(DoubleWritable.class);

    		double minValue = (double) min.getValue();
    		double maxValue = (double) max.getValue();
    		job.setDouble("min", minValue);
    		job.setDouble("max", maxValue);
    	}
    	
    	FileInputFormat.setInputPaths(job,inputPath);
    	FileOutputFormat.setOutputPath(job, outputPath);
    	
    	try {
    		FileSystem fs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000/"), conf);
    		
    		if(fs.exists(outputPath)) {
    			fs.delete(outputPath, true);
    		}
    		
    		RunningJob task = JobClient.runJob(job);
    		
    		if(task.isSuccessful()) {
    			Path resultPath = new Path("hdfs://127.0.0.1:9000/result/part-00000");
    			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(resultPath)));
    			String line;
    			result.setText("");
    			
    			 while ((line = reader.readLine()) != null) {
    		            result.append(line + "\n");
    		     }
    			 reader.close();
    		}else {
    			System.out.println("Job went caput!");
    		}
    		
    	}catch(IOException ex) {
    		System.out.println(ex.toString());
    	}
    	
	}
    	
}
