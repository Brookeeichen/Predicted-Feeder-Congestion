#!/usr/bin/env python3
"""
tune_ridge_lasso.py

Search for optimal alpha values for Ridge and Lasso regression
on feeder_features_cleaned_PhillipEdits.csv.
"""

import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

from sklearn.linear_model import Ridge, Lasso
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score


# ======================
# CONFIG
# ======================

DATA_PATH = "outputs/feeder_features_cleaned_PhillipEdits.csv"
TARGET_COL = "mean_ica_sg"
TEST_SIZE = 0.2
RANDOM_STATE = 42
CV_FOLDS = 5


def print_regression_metrics(y_true, y_pred, label=""):
    """Pretty-print regression metrics."""
    mse = mean_squared_error(y_true, y_pred)
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)

    print(f"\n=== {label} ===")
    print(f"RMSE: {rmse:.3f}")
    print(f"MAE:  {mae:.3f}")
    print(f"R²:   {r2:.3f}")


def main():
    # ======================
    # 1. LOAD DATA
    # ======================
    df = pd.read_csv(DATA_PATH)
    print(f"Loaded data from: {DATA_PATH}")
    print("Shape:", df.shape)

    if TARGET_COL not in df.columns:
        raise ValueError(f"Target column '{TARGET_COL}' not found in dataframe!")

    # Replace inf/-inf with NaN so we can drop them cleanly
    df = df.replace([np.inf, -np.inf], np.nan)

    # ======================
    # 2. DEFINE FEATURES (ALL NUMERIC)
    # ======================
    numeric_cols = [
        col for col in df.select_dtypes(include=[np.number]).columns
        if col != TARGET_COL
    ]

    feature_cols = numeric_cols
    print("\nUsing numeric feature columns:")
    print(feature_cols)

    # Drop rows with missing values in features or target
    model_df = df[feature_cols + [TARGET_COL]].dropna()

    X = model_df[feature_cols]
    y = model_df[TARGET_COL]

    print("\nAfter dropping missing values:")
    print("X shape:", X.shape)
    print("y length:", y.shape[0])

    # ======================
    # 3. TRAIN / TEST SPLIT
    # ======================
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
    )

    # ======================
    # 4. PREPROCESSOR (ONLY STANDARD SCALER)
    # ======================
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numeric_cols),
        ],
        remainder="drop",
    )

    # ======================
    # 5. RIDGE: TUNE ALPHA
    # ======================
    ridge_pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", Ridge()),
        ]
    )

    ridge_alphas = np.logspace(-3, 4, 15)  # 0.001 → 10000
    ridge_param_grid = {"model__alpha": ridge_alphas}

    ridge_search = GridSearchCV(
        ridge_pipeline,
        ridge_param_grid,
        cv=CV_FOLDS,
        scoring="neg_mean_squared_error",
        n_jobs=-1,
        verbose=1,
    )

    print("\nFitting Ridge grid search...")
    ridge_search.fit(X_train, y_train)

    print("\nBest Ridge alpha:", ridge_search.best_params_["model__alpha"])
    print("Best Ridge CV RMSE:", np.sqrt(-ridge_search.best_score_))

    # Evaluate best Ridge on test set
    best_ridge = ridge_search.best_estimator_
    y_pred_ridge = best_ridge.predict(X_test)
    print_regression_metrics(y_test, y_pred_ridge, label="Best Ridge (test set)")

    # ======================
    # 6. LASSO: TUNE ALPHA
    # ======================
    lasso_pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", Lasso(max_iter=20000)),
        ]
    )

    lasso_alphas = np.logspace(-3, 4, 15)  # 0.001 → 10000
    lasso_param_grid = {"model__alpha": lasso_alphas}

    lasso_search = GridSearchCV(
        lasso_pipeline,
        lasso_param_grid,
        cv=CV_FOLDS,
        scoring="neg_mean_squared_error",
        n_jobs=-1,
        verbose=1,
    )

    print("\nFitting Lasso grid search...")
    lasso_search.fit(X_train, y_train)

    print("\nBest Lasso alpha:", lasso_search.best_params_["model__alpha"])
    print("Best Lasso CV RMSE:", np.sqrt(-lasso_search.best_score_))

    # Evaluate best Lasso on test set
    best_lasso = lasso_search.best_estimator_
    y_pred_lasso = best_lasso.predict(X_test)
    print_regression_metrics(y_test, y_pred_lasso, label="Best Lasso (test set)")

    print("\nDone tuning Ridge and Lasso.")


if __name__ == "__main__":
    main()
