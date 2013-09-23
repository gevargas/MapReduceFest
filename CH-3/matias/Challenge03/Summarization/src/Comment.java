import java.util.Date;

public class Comment
{
	private final int mId;
	private final String mText;
	private final int mUserId;
	private final Date mCreationDate;

	public Comment(int id, String text, int userId, Date creationDate)
	{
		mId = id;
		mText = text;
		mUserId = userId;
		mCreationDate = creationDate;
	}

	public int getId() { return mId; }
	public String getText() { return mText; }
	public int getUserId() { return mUserId; }
	public Date getCreationDate() { return mCreationDate; }
}
