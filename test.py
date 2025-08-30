text = "This is a BUNCH of WordsASDFASDF"

def get_syllables(text):
    items_to_remove = ",.-;:!?()[]{}\"'`~@#$%^&*_+=|\\/<>"
    for item in items_to_remove:
        text = text.replace(item, "")
    
    words = str(text).split()
    syllables = 0
    for word in words:
        syllables += get_syllables_in_word(word.lower())
    return syllables
    

def get_syllables_in_word(word):
    vowels = "aeiouAEIOU"
    syllable_count = 0
    in_vowel_sequence = False

    for char in word:
        if char in vowels:
            if not in_vowel_sequence:
                syllable_count += 1
                in_vowel_sequence = True
        else:
            in_vowel_sequence = False

    # Adjust for silent 'e' at the end
    if word.endswith("e") or word.endswith("E"):
        if syllable_count > 1:
            syllable_count -= 1

    return syllable_count