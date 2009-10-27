/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.tools.streams;

/**
 * StreamGenerator.java is a 1.4 application that 
 * demonstrates the use of JButton, JTextField and
 * JLabel.  It requires no other files.
 */

import java.awt.*;
import java.awt.event.*;
import java.io.PrintWriter;
import java.util.Random;

import javax.swing.*;

public class StreamGenerator implements ActionListener {
    JFrame generatorFrame;
    JPanel generatorPanel;
    JTextField fileField, formatField, numberField;
    JLabel fileLabel, formatLabel, numberLabel;
    JButton generateButton;
    PrintWriter out = null;
    int[] formats;
    Random random = new Random();
    
    public StreamGenerator() {
        //Create and set up the window.
        generatorFrame = new JFrame("Stream Generator");
        generatorFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        generatorFrame.setSize(new Dimension(120, 40));

        //Create and set up the panel.
        generatorPanel = new JPanel(new GridLayout(4, 4));

        //Add the widgets.
        addWidgets();

        //Set the default button.
        generatorFrame.getRootPane().setDefaultButton(generateButton);

        //Add the panel to the window.
        generatorFrame.getContentPane().add(generatorPanel, BorderLayout.CENTER);

        //Display the window.
        generatorFrame.pack();
        generatorFrame.setVisible(true);
    }

    /**
     * Create and add the widgets.
     */
    private void addWidgets() {
        //Create widgets.
        fileField = new JTextField(10);
        formatField = new JTextField(10);
        numberField = new JTextField(5);
        fileLabel = new JLabel("File", SwingConstants.LEFT);
        generateButton = new JButton("Generate");
        formatLabel = new JLabel("Format", SwingConstants.LEFT);
        numberLabel = new JLabel("Number of tuples", SwingConstants.LEFT);

        //Listen to events from the Convert button.
        generateButton.addActionListener(this);

        //Add the widgets to the container.
        generatorPanel.add(fileLabel);
        generatorPanel.add(fileField);
        generatorPanel.add(formatLabel);
        generatorPanel.add(formatField);
        generatorPanel.add(numberLabel);
        generatorPanel.add(numberField);
        generatorPanel.add(generateButton);
        

        fileLabel.setBorder(BorderFactory.createEmptyBorder(5,5,5,5));
        formatLabel.setBorder(BorderFactory.createEmptyBorder(5,5,5,5));
    }

    public void actionPerformed(ActionEvent event) {
        
        if (out == null){
            
            try{
                String fileName = fileField.getText();
                String format = formatField.getText();
                out = new PrintWriter(fileName);
                String[] formats = format.split(",");
                this.formats = new int[formats.length];
                for (int i=0; i<formats.length; i++){
                    this.formats[i] = (int)Math.pow(10,Integer.parseInt(formats[i]));
                }
            }catch(Exception e){
                new Dialog(generatorFrame,"Input not well formed");
            }
            
            //First time
            fileField.setEditable(false);
            formatField.setEditable(false);

        }

        int numTuples=0;
        
        try{
            numTuples = Integer.parseInt(numberField.getText()); 
        }catch(Exception e){
            new Dialog(generatorFrame,"Input not well formed");
        }

        for (int i=0; i<numTuples; i++){
            for (int j=0; j<formats.length; j++){
                out.print(random.nextInt(formats[j]));
                if (j==formats.length-1)
                    out.println("");
                else
                    out.print("\t");
            }
        }
        
        out.flush();
        
    }

    /**
     * Create the GUI and show it.  For thread safety,
     * this method should be invoked from the
     * event-dispatching thread.
     */
    private static void createAndShowGUI() {
        //Make sure we have nice window decorations.
        JFrame.setDefaultLookAndFeelDecorated(true);
    }

    public static void main(String[] args) {
        //Schedule a job for the event-dispatching thread:
        //creating and showing this application's GUI.
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                createAndShowGUI();
            }
        });
    }
}

