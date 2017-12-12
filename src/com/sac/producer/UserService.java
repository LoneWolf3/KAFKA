package com.sac.producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserService {

	// Pairs of username and id
	private Map<String, Integer> usersMap;

	public UserService() {
		usersMap = new HashMap<>();
		usersMap.put("a", 1);
		usersMap.put("b", 2);
		usersMap.put("c", 3);
		usersMap.put("d", 4);
		usersMap.put("e", 5);
		usersMap.put("f", 6);
		
	}

	public Integer findUserId(String userName) {
		return usersMap.get(userName);
	}

	public List<String> findAllUsers() {
		return new ArrayList<>(usersMap.keySet());

	}

}
