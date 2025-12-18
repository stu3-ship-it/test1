class RatingSystem:
    def __init__(self):
        # 定義維度與權重
        self.criteria = {
            "skill": 0.5,
            "communication": 0.3,
            "punctuality": 0.2
        }
    
    def calculate_score(self, scores):
        """
        scores: dict, 例如 {"skill": 8, "communication": 9, "punctuality": 10}
        """
        if sum(self.criteria.values()) != 1.0:
            raise ValueError("權重總和必須為 1.0")
            
        final_score = 0
        for criterion, weight in self.criteria.items():
            score = scores.get(criterion, 0) # 如果沒打分則默認 0
            final_score += score * weight
            
        return round(final_score, 2)

    def get_rank(self, final_score):
        if final_score >= 9: return "S - 卓越"
        elif final_score >= 8: return "A - 優秀"
        elif final_score >= 6: return "B - 合格"
        else: return "C - 需改進"

# 使用範例
rater = RatingSystem()
candidate_scores = {"skill": 8, "communication": 7, "punctuality": 10}

result = rater.calculate_score(candidate_scores)
rank = rater.get_rank(result)

print(f"最終得分: {result}")  # 輸出: 8.1
print(f"評級: {rank}")       # 輸出: A - 優秀