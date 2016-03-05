package udacity.storm;

import java.io.Serializable;
import java.util.ArrayList;

public class LearningLog<T1, T2> implements Serializable
{
	private static final long serialVersionUID = 1L;

	public Integer questionType;
	public Integer page;
	public ArrayList<T1> answer;
	public ArrayList<T2> result;
}