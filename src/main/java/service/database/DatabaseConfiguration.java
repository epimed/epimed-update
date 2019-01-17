package service.database;

public class DatabaseConfiguration {

	public String host;
	public String port;
	public String username;
	public String password;
	
	public DatabaseConfiguration() {
		super();
	}
	
	public DatabaseConfiguration(String host, String port, String username, String password) {
		super();
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public String toString() {
		return "ConfigurationDatabase [host=" + host + ", port=" + port + ", username=" + username + "]";
	}
	
	
}
