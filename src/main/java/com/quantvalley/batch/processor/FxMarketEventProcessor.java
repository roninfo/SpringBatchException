package com.quantvalley.batch.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ItemProcessor;

import com.quantvalley.batch.model.FxMarketEvent;
import com.quantvalley.batch.model.Trade;

/**
 * The Class FxMarketEventProcessor.
 * 
 * @author ashraf
 */
public class FxMarketEventProcessor implements ItemProcessor<FxMarketEvent, Trade> {

	private static final Logger logger = LoggerFactory.getLogger(FxMarketEventProcessor.class);

	@Override
	public Trade process(final FxMarketEvent fxMarketEvent) throws Exception {

		final String stock = fxMarketEvent.getStock();
		final String time = fxMarketEvent.getTime();
		final double price = Double.valueOf(fxMarketEvent.getPrice());
		final long shares = Long.valueOf(fxMarketEvent.getShares());
		final Trade trade = new Trade(stock, time, price, shares);

		logger.trace("Converting (" + fxMarketEvent + ") into (" + trade + ")");

		return trade;
	}

}
