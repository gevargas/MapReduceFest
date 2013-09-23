import java.util.Locale;
import java.util.Map;
import java.util.HashMap;

class Utils
{
	// This helper function parses the XML line into a Map.
	public static Map<String, String> transformXmlToMap(String xml)
	{
		Map<String, String> map = new HashMap<String, String>();
		try
		{
			// Exploit the fact that splitting on double quote tokenizes the data nicely for us
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2)
			{
				String key = tokens[i].trim();
				String val = tokens[i + 1];
				map.put(key.substring(0, key.length() - 1), val);
			}
		}
		catch (StringIndexOutOfBoundsException e)
		{
			System.err.println(xml);
		}
		
		return map;
	}
	
	public static Integer getIntProperty(Map<String, String> props, String propName)
	{
		String strValue = props.get(propName);
		if (strValue != null && strValue.length() != 0)
		{
			try
			{
				return Integer.parseInt(strValue);
			}
			catch (NumberFormatException e) { }
		}
		
		return null;
	}
	
	public static String getCleanWord(String inWord)
	{
		return inWord.toLowerCase(Locale.US).replace("'", "").replace(".", "").replace(",", "");
	}
}			
