#### Step 1 : Data Extraction from Guardian APIs
Firsly, we need to generate data locally to train our model. We can use the local_data_collection.py and specify the dates and the key. This code will create the specified dataset(headers : id,text,label) and fix the category and labels file. The code will automatically increment the dates and keep on collecting the data specified by the iterations.

To use this API, you will need to sign up for API key here:<br/>
https://open-platform.theguardian.com/access/

There are multiple categories such as Australia news, US news, Football, World news, Sport, Television & radio, Environment, Science, Media, News, Opinion, Politics, Business, UK news, Society, Life and style, Inequality, Art and design, Books, Stage, Film, Music, Global, Food, Culture, Community, Money, Technology, Travel, From the Observer, Fashion, Crosswords, Law.
