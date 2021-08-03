package denodo_integration_mojo;
import java.io.File;
import java.io.IOException;
import java.sql.Array;

//package ai.h2o.mojos.db;

//import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import com.denodo.vdb.engine.storedprocedure.AbstractStoredProcedure;
import com.denodo.vdb.engine.storedprocedure.DatabaseEnvironment;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureException;
import com.denodo.vdb.engine.storedprocedure.StoredProcedureParameter;

import ai.h2o.mojos.runtime.MojoPipeline;
import ai.h2o.mojos.runtime.api.MojoPipelineService;
import ai.h2o.mojos.runtime.frame.MojoFrame;
import ai.h2o.mojos.runtime.frame.MojoFrameBuilder;
import ai.h2o.mojos.runtime.frame.MojoRowBuilder;
import ai.h2o.mojos.runtime.lic.LicenseException;

//import static ai.h2o.mojos.db.Utils.createConnection;
//import static ai.h2o.mojos.db.Utils.isEmpty;

public class DenodoMojoScorer extends AbstractStoredProcedure {

private DatabaseEnvironment environment;  
  
  private int numRowsScored = 0;
  private int numRowsRead = 0;
  private int numRowError = 0;

  public DenodoMojoScorer()
  {}
  
  public void initialize(DatabaseEnvironment theEnvironment) {
	  super.initialize(theEnvironment);
	  this.environment = theEnvironment;
	  
	  
  }
  public String getDescription() {
  	// TODO Auto-generated method stub
  	return "MOJO scorer provides predictions for the custom query table.";
  }

  @Override
  public StoredProcedureParameter[] getParameters() {
  	// TODO Auto-generated method stub
	  return new StoredProcedureParameter[] {
			  new StoredProcedureParameter("Default Next Month Predictions", Types.ARRAY, StoredProcedureParameter.DIRECTION_OUT,
	                    true) };
		/*
		 * , new StoredProcedureParameter[] { new
		 * StoredProcedureParameter("default.next.month.0", Types.DOUBLE,
		 * StoredProcedureParameter.DIRECTION_OUT), new
		 * StoredProcedureParameter("default.next.month.1", Types.DOUBLE,
		 * StoredProcedureParameter.DIRECTION_OUT) }
		 */
  }

  

  @Override
  public String getName() {
  	// TODO Auto-generated method stub
  	return "DenodoMojoScorer";
  }
  public void doCall(Object[] inputValues) throws StoredProcedureException {
	  
	  String licensePath = "/opt/denodo/denodo-platform-8.0/data/license.sig";
	  System.setProperty("ai.h2o.mojos.runtime.license.file", licensePath);
	  // prints Java Runtime Version after property set
      //System.out.print("New : ");
      //System.out.println(System.getProperty("ai.h2o.mojos.runtime.license.file" ));
	  
		/*
		 * try { FileReader fileReader = new FileReader(licensePath); fileReader.read();
		 * fileReader.close(); } catch (Exception e) {
		 * this.getEnvironment().log(LOG_ERROR,e.getMessage()); e.printStackTrace(); }
		 * 
		 * try { FileReader fileReader = new
		 * FileReader("/opt/denodo/denodo-platform-8.0/data/pipeline.mojo");
		 * fileReader.read(); fileReader.close(); } catch (Exception e) {
		 * this.getEnvironment().log(LOG_ERROR,e.getMessage()); e.printStackTrace(); }
		 */
	 Object[] row = new Object[1];
	 ResultSet rs = null;
	 
	 
	  
	  try { 
		  
		  String query = "select * from mojodb.creditcard_test";
	  
	 rs = this.getEnvironment().executeQuery(query); 
	
	 
	 MojoPipeline model =null;
	  try { 
		   model = MojoPipelineService.loadPipeline(new File("/opt/denodo/denodo-platform-8.0/data/pipeline_min2.mojo"));
				   //MojoPipeline.loadFrom("/opt/denodo/denodo-platform-8.0/data/pipeline_185.mojo");
				  
		  //model = MojoPipelineService.loadPipeline(new File("pipeline.mojo"));
	  } catch (IOException e) {
	  this.getEnvironment().log(LOG_ERROR,e.getMessage()); e.printStackTrace(); }
	    catch (LicenseException e) 
	  {  this.getEnvironment().log(LOG_ERROR,e.getMessage());e.printStackTrace(); }
	 
	
    MojoFrameBuilder frameBuilder = model.getInputFrameBuilder();
    MojoRowBuilder rowBuilder = frameBuilder.getMojoRowBuilder();
    MojoFrame iframe, oframe = null;

    //@SuppressWarnings("deprecation")
	String[] features = model.getInputMeta().getColumnNames();
    try {
		if (rs.last()) {
			  numRowsRead = rs.getRow();
			  rs.beforeFirst(); // not rs.first() because the rs.next() below will move on, missing the first element
			}
	} catch (SQLException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
       
    List<Row> table = new ArrayList<Row>();

    Row row1 = null;

    try {
		Row.formTable(rs, table);
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   
          // Fill row builder
    for (Row rowObj : table)
    {
    	String rowStr = "" ;
        for (Entry<Object, Class> col: rowObj.row)
        {
            rowStr = rowStr+","+col.getValue().toString();
        }
 
    String rowId;
    String[] fileline = rowStr.split(",");
    rowId = fileline[0];
    parseRow(rowStr, fileline, features, rowBuilder); // Make a prediction
    frameBuilder = model.getInputFrameBuilder();
    frameBuilder.addRow(rowBuilder);
   
    }
    iframe = frameBuilder.toMojoFrame();
    oframe = model.transform(iframe);
    
    
    List<Struct> compoundField = new ArrayList<Struct>();
    List<String> fieldsNames = Arrays.asList("default.next.month.0", "default.next.month.1");
    /*
     * 'values' was generated before
     */
    for (int i =0; i< oframe.getNrows(); i++) {

        List<Object> structValues = Arrays.asList(oframe.getColumnName(0).toString()
                                          , oframe.getColumnData(i));
        Struct struct = null;
		try {
			struct = super.createStruct(fieldsNames, structValues);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        compoundField.add(struct);
		 
    }
	/*
	 * List<Row> outputFrame = new ArrayList<Row>();
	 * copyResultFields(oframe,outputFrame);
	 */
    row[0] = createArray(compoundField,Types.STRUCT);
    
    getProcedureResultSet().addRow(row);
    
    /*for(int i =0; i< oframe.getNrows(); i++) 
    {
    getProcedureResultSet().addRow(new Object[] {outputFrame.get(i)});
    }*/
	  }catch (StoredProcedureException e){
		  this.getEnvironment().log(LOG_ERROR,e.getMessage());
		  throw e;
		  }finally {
			  if(rs!=null)
			  {try {
				  rs.close();
			  }catch(SQLException e) {
		  }
			  }
		  }
          
          
  }
         
       
 private boolean parseRow(String row,
                           String[] fileline,
                           String[] features,
                           MojoRowBuilder rowBuilder) {
    assert rowBuilder.size() == features.length : "RowBuilder does not match number of input features!";

     
    if (fileline.length != features.length + 1) {
      numRowError++;
      return false;
    }
    int fieldErrors = 0;
    for (int f = 0; f < rowBuilder.size(); f++) {
      if (fileline.length <= (f + 1)) {
        // parsing can be very strange.... for some records
        rowBuilder.setValue(features[f], "");
      } else {
        if (fileline[1 + f].toLowerCase().equals("null")) {
          rowBuilder.setValue(features[f], "");
        } else {
          try {
            rowBuilder.setValue(features[f], fileline[1 + f]);
          } catch (Exception ex) {
            numRowError++;
            fieldErrors++;
            continue;
          }
        }

      }
    }
    return fieldErrors == 0;
  }

  static String normalizeColumnName(String name) {
    return name.replace('.', '_').replace('-', '_');
  }

private static void copyResultFields(MojoFrame mojoFrame, List<Row> outputRows) {
    String[][] outputColumns = new String[mojoFrame.getNcols()][];
    for (int col = 0; col < mojoFrame.getNcols(); col++) {
      outputColumns[col] = mojoFrame.getColumn(col).getDataAsStrings();
    }
    for (int row = 0; row < mojoFrame.getNrows(); row++) {
      Row outputRow = outputRows.get(row);
      for (String[] resultColumn : outputColumns) {
        outputRow.add(resultColumn[row]);
      }
    }
}
}
    