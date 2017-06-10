package com.example.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

public class TimeStamp extends EvalFunc<Tuple> {
    LookupService cl;
	@Override
	public Tuple exec(Tuple t) throws IOException {
	    if (cl == null) {
	        cl = new LookupService("GeoLiteCity.dat",
	                LookupService.GEOIP_MEMORY_CACHE );
	    }
	    Location loc = cl.getLocation((String)t.get(0));
	    
	    String stp = (String)t.get(1);
	    String timestamp = stp.substring(1);
	    String[] stpslice = timestamp.split("-");
	    String year = stpslice[0];
	    String month = stpslice[1];
	    String date = stpslice[2];
	    
	    if (loc != null && year != null && month != null && date != null){
		    Tuple result = TupleFactory.getInstance().newTuple();
		    result.append(year);
		    result.append(month);
		    result.append(date);
		    result.append(loc.countryName);
		    result.append(loc.city);
		    return result;
	    }else{
	    	return null;
	    }
	}
	@Override
    public List<String> getShipFiles() {
        List<String> shipFiles = new ArrayList<String>();
        shipFiles.add("GeoLiteCity.dat");
        return shipFiles;
    }
	@Override
    public Schema outputSchema(Schema input) {
	    try {
            return Utils.getSchemaFromString("(year:chararray, month:chararray,date:chararray, country:chararray, city:chararray)");
        } catch (ParserException e) {
            throw new RuntimeException(e);
        }
    }
}