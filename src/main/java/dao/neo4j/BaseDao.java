package dao.neo4j;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;

public class BaseDao {

	protected Transaction tx;

	public BaseDao() {
		super();
	}


	public BaseDao(Transaction tx) {
		super();
		this.tx = tx;
	}
	

	public Transaction getTx() {
		return tx;
	}



	public void setTx(Transaction tx) {
		this.tx = tx;
	}



	/** =============================================================================== */

	protected List<Node> convertToList(StatementResult result, String nodeName) {

		List<Node> list = new ArrayList<Node>();

		while (result.hasNext())
		{
			Record record = result.next();
			Node node = record.get(nodeName).asNode();
			list.add(node);
		}

		return list;
	}
	
	/** =============================================================================== */

	protected List<Integer> convertToListInteger(StatementResult result, String key) {

		List<Integer> list = new ArrayList<Integer>();

		while (result.hasNext())
		{
			Record record = result.next();
			Integer value = record.get(key).asInt();
			list.add(value);
		}

		return list;
	}
	
	/** =============================================================================== */

	protected List<String> convertToListString(StatementResult result, String key) {

		List<String> list = new ArrayList<String>();

		while (result.hasNext())
		{
			Record record = result.next();
			String value = record.get(key).asString();
			list.add(value);
		}

		return list;
	}
	
	/** =============================================================================== */

	protected Node convertToNode(StatementResult result, String nodeName) {

		Node node = null;

		if (result.hasNext())
		{
			Record record = result.next();
			node = record.get(nodeName).asNode();
		}

		return node;
	}

	/** =============================================================================== */

	protected Relationship convertToRelationship(StatementResult result, String relName) {

		Relationship rel = null;

		if (result.hasNext())
		{
			Record record = result.next();
			rel = record.get(relName).asRelationship();
		}

		return rel;
	}
	
	/** =============================================================================== */

	protected Path convertToPath(StatementResult result, String pathName) {

		Path path = null;

		if (result.hasNext())
		{
			Record record = result.next();
			path = record.get(pathName).asPath();
		}

		return path;
	}

	/** =============================================================================== */

	protected List<Path> convertToListOfPaths(StatementResult result, String pathName) {

		List<Path> list = new ArrayList<Path>();

		while (result.hasNext())
		{
			Record record = result.next();
			Path path = record.get(pathName).asPath();
			list.add(path);
		}

		return list;
	}
	
	/** =============================================================================== */
}
