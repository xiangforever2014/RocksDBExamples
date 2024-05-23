package utils;
import java.util.Iterator;

public class DataGenerator implements Iterable<String> {
	private long datanum;
	private int digitnum;

	public DataGenerator(long datanum) {
		this.datanum = datanum;
		this.digitnum = (int)(Math.log10(datanum) + 1);
	}

	@Override
	public Iterator<String> iterator() {
		return new DataIterator();
	}

	private class DataIterator implements Iterator<String> {
		private long current = 0;

		@Override
		public boolean hasNext() {
			return current < datanum;
		}

		@Override
		public String next() {
			String str = String.format("%0" + digitnum + "d", current);
			current++;
			if (current % 1000000 == 0) {
				System.out.println("Generated " + current + " data.");
			}
			return str;
		}
	}

	public static void main(String[] args) {
		DataGenerator dataGenerator = new DataGenerator(10);
		Iterator<String> iterator = dataGenerator.iterator();
		while (iterator.hasNext()) {
			System.out.println(iterator.next());
		}
	}
}