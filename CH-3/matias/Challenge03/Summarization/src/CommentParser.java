import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

public class CommentParser
{
	private final static SimpleDateFormat sDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	private final static DocumentBuilderFactory sBuilderFactory = DocumentBuilderFactory.newInstance();
	
	public static Comment parse(String line)
	{
		try
		{
			DocumentBuilder docBuilder = sBuilderFactory.newDocumentBuilder();
			Document document = docBuilder.parse(new InputSource(new StringReader(line)));
			Element element = document.getDocumentElement(); 

			// Sample comment:
			// <row Id="2" PostId="10" Score="1" Text="There is ..." CreationDate="2010-09-13T23:47:12.287" UserId="102" />
			int id = Integer.parseInt(element.getAttribute("Id"));
			String text = element.getAttribute("Text");
			Date creationDate = sDateFormat.parse(element.getAttribute("CreationDate"));
			
			int userId = -1;
			if (element.hasAttribute("UserId")) // Not all comments have UserId!
				userId = Integer.parseInt(element.getAttribute("UserId"));
			
			return new Comment(id, text, userId, creationDate);
		}
		catch (Exception e)
		{
			return null;
		}
	}
}
