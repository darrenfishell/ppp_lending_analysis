import pandas as pd
from scipy import stats
from statsmodels.stats.diagnostic import het_breuschpagan
from statsmodels.stats.outliers_influence import variance_inflation_factor
from statsmodels.tools.tools import add_constant
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def evaluate_vifs(model):
    dataframe = pd.DataFrame(model.model.exog, columns=model.model.exog_names)

    pred_x = dataframe.select_dtypes(include=[np.number])
    pred_x = add_constant(pred_x)

    # Calculate VIF for each feature
    vif_df = pd.DataFrame()
    vif_df['Feature'] = pred_x.columns
    vif_df['VIF'] = [variance_inflation_factor(pred_x.values, i) for i in range(pred_x.shape[1])]
    return vif_df.sort_values(by='VIF', ascending=False)

def test_model_assumptions(model):
    # Get residuals from your model
    residuals = model.resid
    X_design = model.model.exog

    bp_stat, bp_pvalue, _, _ = het_breuschpagan(model.resid, X_design)
    print(f'bp_stat: {bp_stat} | bp_pvalue: {bp_pvalue}')

    # Create diagnostic plots
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))

    # 1. Histogram
    axes[0, 0].hist(residuals, bins=50, density=True, alpha=0.7, color='skyblue')
    axes[0, 0].set_title('Distribution of Residuals')
    axes[0, 0].set_xlabel('Residuals')

    # 2. Q-Q Plot
    stats.probplot(residuals, dist="norm", plot=axes[0, 1])
    axes[0, 1].set_title('Q-Q Plot (Normal)')

    # 3. Residuals vs Fitted
    fitted = model.fittedvalues
    axes[1, 0].scatter(fitted, residuals, alpha=0.6)
    axes[1, 0].axhline(y=0, color='red', linestyle='--')
    axes[1, 0].set_xlabel('Fitted Values')
    axes[1, 0].set_ylabel('Residuals')
    axes[1, 0].set_title('Residuals vs Fitted')

    # 4. Box plot
    axes[1, 1].boxplot(residuals)
    axes[1, 1].set_title('Box Plot of Residuals')

    plt.tight_layout()
    plt.show()

def plot_correlation_matrix(data=None, fig_size=(10, 8), annot=True):
    numeric_cols = data.select_dtypes(include=[np.number]).columns
    correlation_matrix = data[numeric_cols].corr()

    plt.figure(figsize=fig_size)
    sns.heatmap(correlation_matrix,
                annot=annot,
                cmap='coolwarm',
                center=0,
                square=False,
                linewidths=0.3
                )
    plt.title('Correlation Matrix')
    plt.tight_layout()
    plt.show()


def generate_formula_string(target=None, data=None, exclusion_set=None, cat_var_set=None, transform=None):
    feature_set = set(data.select_dtypes(include=[np.number]).columns)

    # Remove target
    feature_set.discard(target)

    if exclusion_set:
        feature_set = feature_set - exclusion_set

    if cat_var_set:
        feature_set = feature_set | cat_var_set

    if transform == 'log':
        tgt_expression = 'np.log(' + target + ')'
    else:
        tgt_expression = target

    feature_str = ' + '.join(sorted(feature_set))
    return target + ' ~ ' + feature_str