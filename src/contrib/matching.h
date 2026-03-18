// File:        matching.h, derived from orginal match.cpp
// Author:      Robert A. van Engelen, engelen@genivia.com
// Date:        August 5, 2019
// License:     The Code Project Open License (CPOL)
//              https://www.codeproject.com/info/cpol10.aspx

#include <string>

namespace utils
{
// returns TRUE if text string matches gitignore-style glob pattern. match is case sensitive
bool gitignore_glob_match(const std::string& text, const std::string& glob)
{
	// enable dotglob: *. ?, and [] match a . (dotfile) at the begin or after each /
	constexpr bool dotglob = true;
	constexpr char pathsep = '/';

	size_t i = 0;
	size_t j = 0;
	const size_t n = text.size();
	const size_t m = glob.size();
	size_t text1_backup = std::string::npos;
	size_t glob1_backup = std::string::npos;
	size_t text2_backup = std::string::npos;
	size_t glob2_backup = std::string::npos;
	bool nodot = !dotglob;

	// match pathname if glob contains a / otherwise match the basename
	if (j + 1 < m && glob[j] == '/')
	{
		// if pathname starts with ./ then ignore these pairs
		while (i + 1 < n && text[i] == '.' && text[i + 1] == pathsep)
		{
			i += 2;
		}
		// if pathname starts with a / then ignore it
		if (i < n && text[i] == pathsep)
		{
			i++;
		}
		j++;
	}
	else if (glob.find('/') == std::string::npos)
	{
		size_t sep = text.rfind(pathsep);
		if (sep != std::string::npos)
		{
			i = sep + 1;
		}
	}
	while (i < n)
	{
		const char text_i = text[i];
		if (j < m)
		{
			switch (glob[j])
			{
			case '*':
				// match anything except . after /
				if (nodot && text_i == '.')
					break;
				if (++j < m && glob[j] == '*')
				{
					// trailing ** match everything after /
					if (++j >= m)
					{
						return true;
					}
					// ** followed by a / match zero or more directories
					if (glob[j] != '/')
					{
						return false;
					}
					// new **-loop, discard *-loop
					text1_backup = std::string::npos;
					glob1_backup = std::string::npos;
					text2_backup = i;
					glob2_backup = j;
					if (text_i != '/')
					{
						j++;
					}
					continue;
				}
				// trailing * matches everything except /
				text1_backup = i;
				glob1_backup = j;
				continue;
			case '?':
				// match anything except . after /
				if (nodot && text_i == '.')
				{
					break;
				}
				// match any character except /
				if (text_i == pathsep)
				{
					break;
				}
				i++;
				j++;
				continue;
			case '[':
			{
				// match anything except . after /
				if (nodot && text_i == '.')
				{
					break;
				}
				// match any character in [...] except /
				if (text_i == pathsep)
				{
					break;
				}
				bool matched = false;
				const bool reverse = j + 1 < m && (glob[j + 1] == '^' || glob[j + 1] == '!');
				// inverted character class
				if (reverse)
				{
					j++;
				}
				// match character class
				for (int lastchr = 256; ++j < m && glob[j] != ']'; lastchr = static_cast<int>(glob[j]))
				{
					if ((lastchr < 256) && (glob[j] == '-') && (j + 1 < m) && (glob[j + 1] != ']')
						? text_i <= glob[++j] && static_cast<int>(text_i) >= lastchr
						: text_i == glob[j])
					{
						matched = true;
					}
				}
				if (matched == reverse)
				{
					break;
				}
				i++;
				if (j < m)
				{
					j++;
				}
				continue;
			}
			case '\\':
				// literal match \-escaped character
				if (j + 1 < m)
				{
					j++;
				}
				// FALLTHROUGH
			default:
				// match the current non-NUL character
				const char glob_j = glob[j];
				if (glob_j != text_i && !(glob_j == '/' && text_i == pathsep))
				{
					break;
				}
				// do not match a . with *, ? [] after /
				nodot = !dotglob && glob_j == '/';
				i++;
				j++;
				continue;
			}
		}
		if (glob1_backup != std::string::npos && text[text1_backup] != pathsep)
		{
			// *-loop: backtrack to the last * but do not jump over /
			i = ++text1_backup;
			j = glob1_backup;
			continue;
		}
		if (glob2_backup != std::string::npos)
		{
			// **-loop: backtrack to the last **
			i = ++text2_backup;
			j = glob2_backup;
			continue;
		}
		return false;
	}
	// ignore trailing stars
	while (j < m && glob[j] == '*')
	{
		j++;
	}
	// at end of text means success if nothing else is left to match
	return j >= m;
}

} // namespace utils