#pragma once
#include<unordered_map>
#include<string>
#include "BACH/common/tuple.h"


namespace BACH {
    struct AnswerMerger {
        AnswerMerger() = default;
        ~AnswerMerger() = default;
        // if update = 0, the current Tuple will be eliminated, otherwise, it will replace the answer
        void insert_answer(const std::string &key, Tuple &&x, bool update = false) {
            if(answers.contains(key)) {
                if(update) {
                    answers[key] = x;
                }
                return;
            }
            answers[key] = x;
        }

        std::unordered_map<std::string, Tuple> answers;
    };
}
