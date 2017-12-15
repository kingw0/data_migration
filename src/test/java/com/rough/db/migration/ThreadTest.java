/**
 * @Copyright:2016-2020 www.fixuan.com Inc. All rights reserved.
 */
package com.rough.db.migration;

import org.junit.Test;

public class ThreadTest
{
	@Test
	public void testThread() throws InterruptedException
	{
		Thread t = new Thread(new Runnable()
		{

			@Override
			public void run()
			{
				System.out.println("2." + Thread.currentThread().getState());

				try
				{
					Thread.sleep(4000);
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}

				System.out.println("4." + Thread.currentThread().getState());
			}
		});

		System.out.println("1." + t.getState());

		t.start();

		Thread.sleep(1000);

		System.out.println("3." + t.getState());

		Thread.sleep(5000);

		System.out.println("5." + t.getState());

	}
}
