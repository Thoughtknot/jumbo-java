package org.jumbo.tcp.examples.trading;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Inet4Address;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Scanner;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jumbo.tcp.BytesUtil;
import org.jumbo.tcp.Jumbo;
import org.jumbo.tcp.JumboTcpClient;

public class TradingApplication {
	TreeSet<Order> buyOrders = new TreeSet<>();
	TreeSet<Order> sellOrders = new TreeSet<>();
	List<Trade> trades = new LinkedList<>();
	
	private static final int USER_TABLE = 101;
	private static final int ORDER_TABLE = 102;
	private static final int TRADE_TABLE = 103;
	private Jumbo jumbo;
	private List<User> users;
	
	public TradingApplication(Jumbo jumbo) {
		this.jumbo = jumbo;
	}

	public static void main(String[] args) throws IOException {
		Jumbo jumbo = JumboTcpClient.connect(Inet4Address.getLocalHost(), 8080)
			.blockUntilConnected();
		TradingApplication ta = new TradingApplication(jumbo);
		Scanner s = new Scanner(System.in);
		System.out.println("Open market? (y/n)");
		String res = s.nextLine();
		if (res.equalsIgnoreCase("y")) {
			System.out.println("MARKET OPEN");
			ta.simulateMarket(10);
			System.out.println("MARKET CLOSED");
		}
		else {
			System.out.println("Exiting.");
		}
	}

	private void simulateMarket(int noOrders) throws IOException {
		users = createUsers(noOrders / 3);
		Random r = new Random();
		for (int i = 0; i < noOrders; i++) {
			int qty = r.nextInt(22);
			boolean isBuy = r.nextBoolean();
			double price = r.nextDouble() * 54;
			
			User user = users.get(r.nextInt(users.size()));
			Order order = Order.order(price, qty, user.userId, isBuy);
			enterOrder(order);
		}
		
		System.out.println("Buy orders:");
		for (Order o : buyOrders) {
			Order a = (Order) deserialize(jumbo.get(ORDER_TABLE, BytesUtil.intAsLe(o.seqNo)));
			System.out.println(a);
		}
		System.out.println("Sell orders:");
		for (Order o : sellOrders) {
			Order a = (Order) deserialize(jumbo.get(ORDER_TABLE, BytesUtil.intAsLe(o.seqNo)));
			System.out.println(a);
		}
		System.out.println("Trades:");
		for (Trade t : trades) {
			Trade a = (Trade) deserialize(jumbo.get(TRADE_TABLE, BytesUtil.intAsLe(t.seqNo)));
			System.out.println(a);
		}
	}
	
	private List<User> createUsers(int i) {
		return IntStream.range(0, i)
			.mapToObj(c -> createUser(("USER-" + c)))
			.collect(Collectors.toList());
	}

	private User createUser(String userId) {
		try {
			byte[] val = jumbo.get(USER_TABLE, userId.getBytes());
			if (val.length > 0) {
				System.out.println("User already exists!");
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		User u = new User(userId);
		try {
			jumbo.put(USER_TABLE, userId.getBytes(), serialize(u));
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		return u;
	}

	
	private void enterOrder(Order order) throws IOException {
		updateOrder(order);
		if (order.isBuy) {
			match(sellOrders, order).ifPresent(buyOrders::add);
		}
		else {
			match(buyOrders, order).ifPresent(sellOrders::add);
		}
	}


	private Optional<Order> match(TreeSet<Order> orders, Order order) throws IOException {
		boolean isUpdated = false;
		while (order.qty > 0) {
			if (!orders.isEmpty() && 
					((order.isBuy && (orders.first().price < order.price))
					|| (!order.isBuy && (orders.first().price > order.price)))) {
				isUpdated = true;
				Order first = orders.pollFirst();
				if (first.qty < order.qty) {
					order.qty -= first.qty;
					createTrade(first, order, first.qty);
					first.qty = 0;
					updateOrder(first);
				}
				else {
					first.qty -= order.qty;
					updateOrder(first);
					createTrade(first, order, order.qty);
					order.qty = 0;
				}
			}
			else {
				break;
			}
		}
		if (isUpdated) {
			updateOrder(order);
		}
		if (order.qty == 0) {
			return Optional.empty();
		}
		else {
			return Optional.of(order);
		}
	}

	private void createTrade(Order first, Order order, int qty) throws IOException {
		double price;
		String sellerId;
		String buyerId;
		if (first.isBuy) {
			price = order.price;
			buyerId = first.userId;
			sellerId = order.userId;
		}
		else {
			buyerId = order.userId;
			sellerId = first.userId;
			price = first.price;
		}
		Trade trade = Trade.trade(price, qty, sellerId, buyerId);
		trades.add(trade);
		jumbo.put(TRADE_TABLE, BytesUtil.intAsLe(trade.seqNo), serialize(trade));
	}

	private void updateOrder(Order order) throws IOException {
		jumbo.put(ORDER_TABLE, BytesUtil.intAsLe(order.seqNo), serialize(order));
	}


	public Object deserialize(byte[] a) {
		try {
			ByteArrayInputStream baos = new ByteArrayInputStream(a);
			return new ObjectInputStream(baos).readObject();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public byte[] serialize(Serializable a) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			new ObjectOutputStream(baos).writeObject(a);
			return baos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
			return new byte[0];
		}
	}
	
	public static class User implements Serializable {
		private String userId;
		
		public User(String userId) {
			this.userId = userId;
		}
		
		@Override
		public String toString() {
			return "User[" + userId + "]";
		}
	}

	public static class Trade implements Serializable {
		private static int seqNoCounter = 0;
		private int seqNo;
		private double price;
		private int qty;
		private String buyerId;
		private String sellerId;
		
		private Trade(double price, int qty, String sellerId, String buyerId) {
			this.price = price;
			this.qty = qty;
			this.buyerId = buyerId;
			this.sellerId = sellerId;
			seqNo = seqNoCounter++;
		}
		
		public static Trade trade(double price, int qty, String sellerId, String buyerId) {
			return new Trade(price, qty, sellerId, buyerId);
		}
		
		@Override
		public String toString() {
			return "Trade[" + seqNo + "," + qty + "@" + price + "," + buyerId + "," + sellerId + "]";
		}
	}
	
	public static class Order implements Serializable, Comparable<Order> {
		private static int seqNoCounter = 0;
		private int seqNo;
		private double price;
		private int qty;
		private boolean isBuy;
		private String userId;
		
		private Order(double price, int qty, String userId, boolean isBuy) {
			this.price = price;
			this.qty = qty;
			this.userId = userId;
			this.isBuy = isBuy;
			seqNo = seqNoCounter++;
		}
		
		public static Order order(double price, int qty, String userId, boolean isBuy) {
			return new Order(price, qty, userId, isBuy);
		}

		@Override
		public int compareTo(Order o) {
			if ((price - o.price) == 0) return 0;
			return seqNo - o.seqNo;
		}

		@Override
		public boolean equals(Object obj) {
			Order o = (Order) obj;
			return qty == o.qty 
				&& isBuy == o.isBuy
				&& userId == o.userId
				&& price == o.price
				&& seqNo == o.seqNo;
		}
		
		@Override
		public String toString() {
			return "Order[" + seqNo + "," + qty + "@" + price + "," + isBuy + "," + userId + "]";
		}
	}
}
