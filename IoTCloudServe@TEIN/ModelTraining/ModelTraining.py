"""
SVM Model Training
for offline training the model to predict the number of people 
inside the room (0-3 people) using ambient room sensors. 
Ref: https://www.kaggle.com/code/vivekaryan/room-occupancy-estimation-with-variable-selection 
"""

# Importing relevant modules
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.svm import SVR
from sklearn.metrics import r2_score
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
import joblib

df=pd.read_csv('data_temp.csv')

# Prepare the data
X = []
y = []
for i in range(5, len(df)):
    X.append(df['temp'].iloc[i-5:i].values)
    y.append(df['temp'].iloc[i])
X = np.array(X)
y = np.array(y)

# Split the data into train and test sets with an 80:20 ratio
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train the Support Vector Machine (SVM) regression model
svm_model = SVR(kernel='linear')
svm_model.fit(X_train, y_train)

# Make predictions on the test set
y_pred = svm_model.predict(X_test)

# Calculate Mean Absolute Error (MAE) and Root Mean Squared Error (RMSE)
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r_squared = r2_score(y_test, y_pred)
print("Mean Absolute Error (MAE):", mae)
print("Root Mean Squared Error (RMSE):", rmse)
print("R-squared:", r_squared)

# Export the trained model for use in online prediction. 
svm_model_columns = list(X_train)
print(svm_model_columns)
joblib.dump(svm_model_columns, 'svm_model_columns.pkl')
joblib.dump(svm_model, 'svm_model.pkl')
print('Model dumped')